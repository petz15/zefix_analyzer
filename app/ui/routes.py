import csv
import io
import json
from urllib.parse import quote_plus, urlencode

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app import crud
from app.api.zefix_client import SWISS_CANTONS
from app.config import settings
from app.database import get_db
from app.services.collection import enrich_company_website
from app.schemas.company import CompanyUpdate
from app.schemas.note import NoteCreate, NoteUpdate

router = APIRouter(tags=["ui"])
templates = Jinja2Templates(directory="app/templates")

PAGE_SIZE = 50


def _filter_params(
    q: str | None,
    canton: str | None,
    review_status: str | None,
    proposal_status: str | None,
    google_searched: str | None,
    min_score: int | None,
    sort: str | None,
    industry: str | None,
    tags: str | None,
) -> dict:
    """Build a dict of non-empty filter params for URL construction."""
    p: dict = {}
    if q:
        p["q"] = q
    if canton:
        p["canton"] = canton
    if review_status:
        p["review_status"] = review_status
    if proposal_status:
        p["proposal_status"] = proposal_status
    if google_searched:
        p["google_searched"] = google_searched
    if min_score is not None:
        p["min_score"] = min_score
    if sort:
        p["sort"] = sort
    if industry:
        p["industry"] = industry
    if tags:
        p["tags"] = tags
    return p


def _searched_bool(google_searched: str | None) -> bool | None:
    if google_searched == "yes":
        return True
    if google_searched == "no":
        return False
    return None


@router.get("/", include_in_schema=False)
def root_redirect() -> RedirectResponse:
    return RedirectResponse(url="/ui", status_code=status.HTTP_302_FOUND)


@router.get("/ui", response_class=HTMLResponse, include_in_schema=False)
def ui_home(
    request: Request,
    q: str | None = Query(None),
    canton: str | None = Query(None),
    review_status: str | None = Query(None),
    proposal_status: str | None = Query(None),
    google_searched: str | None = Query(None),
    min_score: int | None = Query(None),
    industry: str | None = Query(None),
    tags: str | None = Query(None),
    sort: str | None = Query(None),
    page: int = Query(1, ge=1),
    message: str | None = Query(None),
    error: str | None = Query(None),
    db: Session = Depends(get_db),
):
    searched_filter = _searched_bool(google_searched)
    filter_kwargs = dict(
        name_filter=q or None,
        canton=canton or None,
        review_status=review_status or None,
        proposal_status=proposal_status or None,
        google_searched=searched_filter,
        min_score=min_score,
        industry=industry or None,
        tags=tags or None,
    )

    companies = crud.list_companies(db, page=page, page_size=PAGE_SIZE,
                                    sort=sort or "-updated", **filter_kwargs)
    total = crud.count_companies(db, **filter_kwargs)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    stats = crud.get_company_stats(db)

    # Build base query string (without page) for pagination links
    fp = _filter_params(q, canton, review_status, proposal_status,
                        google_searched, min_score, sort, industry, tags)
    filter_qs = ("&" + urlencode(fp)) if fp else ""

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "companies": companies,
            "stats": stats,
            "cantons": SWISS_CANTONS,
            # pagination
            "page": page,
            "total_pages": total_pages,
            "total": total,
            "filter_qs": filter_qs,
            # sort
            "sort": sort or "-updated",
            # current filter values
            "f_q": q or "",
            "f_canton": canton or "",
            "f_review_status": review_status or "",
            "f_proposal_status": proposal_status or "",
            "f_google_searched": google_searched or "",
            "f_min_score": min_score if min_score is not None else "",
            "f_industry": industry or "",
            "f_tags": tags or "",
            "google_daily_quota": settings.google_daily_quota,
            "message": message,
            "error": error,
        },
    )


@router.get("/ui/export.csv", include_in_schema=False)
def export_csv(
    q: str | None = Query(None),
    canton: str | None = Query(None),
    review_status: str | None = Query(None),
    proposal_status: str | None = Query(None),
    google_searched: str | None = Query(None),
    min_score: int | None = Query(None),
    industry: str | None = Query(None),
    tags: str | None = Query(None),
    sort: str | None = Query(None),
    db: Session = Depends(get_db),
):
    companies = crud.list_companies(
        db,
        limit=10000,  # export uses legacy limit path
        sort=sort or "-updated",
        name_filter=q or None,
        canton=canton or None,
        review_status=review_status or None,
        proposal_status=proposal_status or None,
        google_searched=_searched_bool(google_searched),
        min_score=min_score,
        industry=industry or None,
        tags=tags or None,
    )

    def _generate():
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "uid", "name", "legal_form", "status", "municipality", "canton",
            "website_url", "website_match_score", "review_status", "proposal_status",
            "contact_name", "contact_email", "contact_phone", "industry", "tags",
            "created_at", "updated_at",
        ])
        yield buf.getvalue()
        for c in companies:
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow([
                c.uid, c.name, c.legal_form or "", c.status or "",
                c.municipality or "", c.canton or "",
                c.website_url or "", c.website_match_score if c.website_match_score is not None else "",
                c.review_status or "", c.proposal_status or "",
                c.contact_name or "", c.contact_email or "", c.contact_phone or "",
                c.industry or "", c.tags or "",
                c.created_at.isoformat(), c.updated_at.isoformat(),
            ])
            yield buf.getvalue()

    return StreamingResponse(
        _generate(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=zefix_leads.csv"},
    )


@router.post("/ui/bulk-update", include_in_schema=False)
async def bulk_update(
    request: Request,
    db: Session = Depends(get_db),
) -> RedirectResponse:
    form = await request.form()
    company_ids_raw = form.getlist("company_ids")
    action = form.get("bulk_action", "")
    # Preserve filters for redirect
    back = str(form.get("back_url", "/ui"))

    if not company_ids_raw or not action:
        return RedirectResponse(
            url=f"{back}&error={quote_plus('Select companies and an action')}",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    company_ids = [int(i) for i in company_ids_raw if i.isdigit()]
    if not company_ids:
        return RedirectResponse(url=back, status_code=status.HTTP_303_SEE_OTHER)

    # action format: "review_status:confirmed" | "proposal_status:sent" | "review_status:_clear"
    if ":" not in action:
        return RedirectResponse(url=back, status_code=status.HTTP_303_SEE_OTHER)

    field, value = action.split(":", 1)
    if value == "_clear":
        value = None

    crud.bulk_update_status(db, company_ids, field, value)
    label = value or "cleared"
    return RedirectResponse(
        url=f"{back}&message={quote_plus(f'{len(company_ids)} companies set to {label}')}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.get("/ui/companies/{company_id}", response_class=HTMLResponse, include_in_schema=False)
def ui_company_detail(
    company_id: int,
    request: Request,
    message: str | None = Query(None),
    error: str | None = Query(None),
    db: Session = Depends(get_db),
):
    company = crud.get_company(db, company_id)
    if not company:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Company not found")

    notes = crud.list_notes_for_company(db, company_id)
    zefix_pretty = None
    if company.zefix_raw:
        try:
            zefix_pretty = json.dumps(json.loads(company.zefix_raw), indent=2, ensure_ascii=True)
        except Exception:  # noqa: BLE001
            zefix_pretty = company.zefix_raw

    google_results: list[dict] = []
    if company.google_search_results_raw:
        try:
            google_results = json.loads(company.google_search_results_raw)
        except Exception:  # noqa: BLE001
            pass

    return templates.TemplateResponse(
        "company_detail.html",
        {
            "request": request,
            "company": company,
            "notes": notes,
            "zefix_pretty": zefix_pretty,
            "google_results": google_results,
            "message": message,
            "error": error,
        },
    )


@router.post("/ui/companies/{company_id}/google-search", include_in_schema=False)
def google_search_for_company(company_id: int, db: Session = Depends(get_db)) -> RedirectResponse:
    company = crud.get_company(db, company_id)
    if not company:
        return RedirectResponse(url="/ui?error=Company+not+found", status_code=status.HTTP_303_SEE_OTHER)

    try:
        enriched, _ = enrich_company_website(db, company)
    except Exception as exc:  # noqa: BLE001
        return RedirectResponse(
            url=f"/ui/companies/{company_id}?error={quote_plus(str(exc))}",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    msg = "Google search complete — results scored and saved" if enriched else "No search results returned"
    return RedirectResponse(
        url=f"/ui/companies/{company_id}?message={quote_plus(msg)}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/companies/{company_id}/edit", include_in_schema=False)
def edit_company(
    company_id: int,
    website_url: str = Form(""),
    review_status: str = Form(""),
    proposal_status: str = Form(""),
    contact_name: str = Form(""),
    contact_email: str = Form(""),
    contact_phone: str = Form(""),
    industry: str = Form(""),
    tags: str = Form(""),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    company = crud.get_company(db, company_id)
    if not company:
        return RedirectResponse(url="/ui?error=Company+not+found", status_code=status.HTTP_303_SEE_OTHER)

    crud.update_company(
        db,
        company,
        CompanyUpdate(
            website_url=website_url.strip() or None,
            review_status=review_status or None,
            proposal_status=proposal_status or None,
            contact_name=contact_name.strip() or None,
            contact_email=contact_email.strip() or None,
            contact_phone=contact_phone.strip() or None,
            industry=industry.strip() or None,
            tags=tags.strip() or None,
        ),
    )
    return RedirectResponse(
        url=f"/ui/companies/{company_id}?message=Company+updated",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/companies/{company_id}/set-website", include_in_schema=False)
def set_website(
    company_id: int,
    website_url: str = Form(...),
    website_match_score: int = Form(0),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    company = crud.get_company(db, company_id)
    if not company:
        return RedirectResponse(url="/ui?error=Company+not+found", status_code=status.HTTP_303_SEE_OTHER)

    crud.update_company(
        db,
        company,
        CompanyUpdate(website_url=website_url.strip() or None, website_match_score=website_match_score),
    )
    return RedirectResponse(
        url=f"/ui/companies/{company_id}?message=Website+updated",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/companies/{company_id}/notes", include_in_schema=False)
def create_note(
    company_id: int,
    content: str = Form(...),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    company = crud.get_company(db, company_id)
    if not company:
        return RedirectResponse(url="/ui?error=Company+not+found", status_code=status.HTTP_303_SEE_OTHER)

    content_clean = content.strip()
    if not content_clean:
        return RedirectResponse(
            url=f"/ui/companies/{company_id}?error=Note+content+cannot+be+empty",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    crud.create_note(db, company_id, NoteCreate(content=content_clean))
    return RedirectResponse(
        url=f"/ui/companies/{company_id}?message=Note+added",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/companies/{company_id}/notes/{note_id}/edit", include_in_schema=False)
def edit_note(
    company_id: int,
    note_id: int,
    content: str = Form(...),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    note = crud.get_note(db, note_id)
    if not note or note.company_id != company_id:
        return RedirectResponse(
            url=f"/ui/companies/{company_id}?error=Note+not+found",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    content_clean = content.strip()
    if not content_clean:
        return RedirectResponse(
            url=f"/ui/companies/{company_id}?error=Note+content+cannot+be+empty",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    crud.update_note(db, note, NoteUpdate(content=content_clean))
    return RedirectResponse(
        url=f"/ui/companies/{company_id}?message=Note+updated",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/companies/{company_id}/notes/{note_id}/delete", include_in_schema=False)
def delete_note(company_id: int, note_id: int, db: Session = Depends(get_db)) -> RedirectResponse:
    note = crud.get_note(db, note_id)
    if not note or note.company_id != company_id:
        return RedirectResponse(
            url=f"/ui/companies/{company_id}?error=Note+not+found",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    crud.delete_note(db, note)
    return RedirectResponse(
        url=f"/ui/companies/{company_id}?message=Note+deleted",
        status_code=status.HTTP_303_SEE_OTHER,
    )
