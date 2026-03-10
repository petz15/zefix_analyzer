import csv
import io
import json
import threading
import time
from urllib.parse import quote_plus, urlencode

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app import crud
from app.api.zefix_client import SWISS_CANTONS
from app.database import SessionLocal, get_db
from app.services.collection import (
    bulk_import_zefix,
    enrich_company_website,
    geocode_and_update_company,
    import_company_from_zefix_uid,
    initial_collect,
    recalculate_google_scores,
    recalculate_zefix_scores,
    run_batch_collect,
    run_zefix_detail_collect,
)
from app.services.scoring import get_default_scoring_config
from app.schemas.company import CompanyUpdate
from app.schemas.note import NoteCreate, NoteUpdate

router = APIRouter(tags=["ui"])
templates = Jinja2Templates(directory="app/templates")
templates.env.filters["tojson_parse"] = lambda s: json.loads(s) if s else {}

PAGE_SIZE = 50


class JobCancelledError(Exception):
    """Raised when a running job receives a cancellation request."""


class JobPausedError(Exception):
    """Raised when a running job receives a pause request."""


def _filter_params(
    q: str | None,
    canton: str | None,
    review_status: str | None,
    proposal_status: str | None,
    google_searched: str | None,
    min_google_score: int | None,
    min_zefix_score: int | None,
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
    if min_google_score is not None:
        p["min_google_score"] = min_google_score
    if min_zefix_score is not None:
        p["min_zefix_score"] = min_zefix_score
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
    min_google_score: str | None = Query(None),
    min_zefix_score: str | None = Query(None),
    industry: str | None = Query(None),
    tags: str | None = Query(None),
    sort: str | None = Query(None),
    page: int = Query(1, ge=1),
    message: str | None = Query(None),
    error: str | None = Query(None),
    db: Session = Depends(get_db),
):
    _ensure_job_worker(request.app)
    min_google_score_int: int | None = int(min_google_score) if min_google_score and min_google_score.strip().lstrip("-").isdigit() else None
    min_zefix_score_int: int | None = int(min_zefix_score) if min_zefix_score and min_zefix_score.strip().lstrip("-").isdigit() else None
    searched_filter = _searched_bool(google_searched)
    filter_kwargs = dict(
        name_filter=q or None,
        canton=canton or None,
        review_status=review_status or None,
        proposal_status=proposal_status or None,
        google_searched=searched_filter,
        min_google_score=min_google_score_int,
        min_zefix_score=min_zefix_score_int,
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
                        google_searched, min_google_score_int, min_zefix_score_int,
                        sort, industry, tags)
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
            "f_min_google_score": min_google_score_int if min_google_score_int is not None else "",
            "f_min_zefix_score": min_zefix_score_int if min_zefix_score_int is not None else "",
            "f_industry": industry or "",
            "f_tags": tags or "",
            "google_search_enabled": crud.get_setting(db, "google_search_enabled", "true") == "true",
            "google_daily_quota": int(crud.get_setting(db, "google_daily_quota", "100")),
            "message": message,
            "error": error,
            "active_task": getattr(request.app.state, "collection_task", None)
                if _task_is_running(request.app.state) else None,
        },
    )


@router.get("/ui/map", response_class=HTMLResponse, include_in_schema=False)
def ui_map(
    request: Request,
    canton: str | None = Query(None),
    review_status: str | None = Query(None),
    google_searched: str | None = Query(None),
    min_google_score: str | None = Query(None),
    min_zefix_score: str | None = Query(None),
    db: Session = Depends(get_db),
):
    return templates.TemplateResponse(
        "map.html",
        {
            "request": request,
            "cantons": SWISS_CANTONS,
            "f_canton": canton or "",
            "f_review_status": review_status or "",
            "f_google_searched": google_searched or "",
            "f_min_google_score": min_google_score or "",
            "f_min_zefix_score": min_zefix_score or "",
        },
    )


from fastapi.responses import JSONResponse  # noqa: E402 (local import to avoid top-level churn)


@router.get("/api/map-data", include_in_schema=False)
def api_map_data(
    canton: str | None = Query(None),
    review_status: str | None = Query(None),
    google_searched: str | None = Query(None),
    min_google_score: int | None = Query(None),
    min_zefix_score: int | None = Query(None),
    db: Session = Depends(get_db),
):
    """Return lightweight JSON for the map — only geocoded companies."""
    from app.models.company import Company as CompanyModel
    query = db.query(
        CompanyModel.id,
        CompanyModel.name,
        CompanyModel.lat,
        CompanyModel.lon,
        CompanyModel.website_match_score,
        CompanyModel.zefix_score,
        CompanyModel.canton,
        CompanyModel.municipality,
        CompanyModel.website_url,
        CompanyModel.review_status,
    ).filter(
        CompanyModel.lat.isnot(None),
        CompanyModel.lon.isnot(None),
    )
    if canton:
        query = query.filter(CompanyModel.canton == canton)
    if review_status:
        query = query.filter(CompanyModel.review_status == review_status)
    searched = _searched_bool(google_searched)
    if searched is True:
        query = query.filter(CompanyModel.website_checked_at.isnot(None))
    elif searched is False:
        query = query.filter(CompanyModel.website_checked_at.is_(None))
    if min_google_score is not None:
        query = query.filter(CompanyModel.website_match_score >= min_google_score)
    if min_zefix_score is not None:
        query = query.filter(CompanyModel.zefix_score >= min_zefix_score)

    rows = query.limit(5000).all()
    features = [
        {
            "id": r.id,
            "name": r.name,
            "lat": r.lat,
            "lon": r.lon,
            "google_score": r.website_match_score,
            "zefix_score": r.zefix_score,
            "canton": r.canton,
            "municipality": r.municipality,
            "website": r.website_url,
            "review": r.review_status,
        }
        for r in rows
    ]
    return JSONResponse({"count": len(features), "features": features})


@router.get("/ui/export.csv", include_in_schema=False)
def export_csv(
    q: str | None = Query(None),
    canton: str | None = Query(None),
    review_status: str | None = Query(None),
    proposal_status: str | None = Query(None),
    google_searched: str | None = Query(None),
    min_google_score: int | None = Query(None),
    min_zefix_score: int | None = Query(None),
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
        min_google_score=min_google_score,
        min_zefix_score=min_zefix_score,
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

    old_names: list = []
    if company.old_names:
        try:
            raw_old = json.loads(company.old_names)
            if isinstance(raw_old, list):
                old_names = [str(n) for n in raw_old if n]
        except Exception:  # noqa: BLE001
            pass

    zefix_score_breakdown: dict | None = None
    if company.zefix_score_breakdown:
        try:
            parsed = json.loads(company.zefix_score_breakdown)
            if isinstance(parsed, dict):
                zefix_score_breakdown = parsed
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
            "old_names": old_names,
            "google_search_enabled": crud.get_setting(db, "google_search_enabled", "true") == "true",
            "zefix_score_breakdown": zefix_score_breakdown,
            "message": message,
            "error": error,
        },
    )


@router.post("/ui/companies/{company_id}/zefix-refresh", include_in_schema=False)
def zefix_refresh_company(company_id: int, db: Session = Depends(get_db)) -> RedirectResponse:
    company = crud.get_company(db, company_id)
    if not company:
        return RedirectResponse(url="/ui?error=Company+not+found", status_code=status.HTTP_303_SEE_OTHER)

    try:
        updated, _ = import_company_from_zefix_uid(db, company.uid)
        geocode_and_update_company(db, updated)
    except Exception as exc:  # noqa: BLE001
        return RedirectResponse(
            url=f"/ui/companies/{company_id}?error={quote_plus(str(exc))}",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    return RedirectResponse(
        url=f"/ui/companies/{company_id}?message={quote_plus('Zefix data refreshed')}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/companies/{company_id}/google-search", include_in_schema=False)
def google_search_for_company(company_id: int, db: Session = Depends(get_db)) -> RedirectResponse:
    if crud.get_setting(db, "google_search_enabled", "true") != "true":
        return RedirectResponse(
            url=f"/ui/companies/{company_id}?error={quote_plus('Google Search is disabled (GOOGLE_SEARCH_ENABLED=false)')}",
            status_code=status.HTTP_303_SEE_OTHER,
        )

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


# ── Settings ──────────────────────────────────────────────────────────────────

@router.get("/ui/settings", response_class=HTMLResponse, include_in_schema=False)
def ui_settings(
    request: Request,
    message: str | None = Query(None),
    error: str | None = Query(None),
    db: Session = Depends(get_db),
):
    _ensure_job_worker(request.app)
    current = crud.get_all_settings(db)

    task = getattr(request.app.state, "collection_task", None)
    active_jobs = crud.list_active_jobs(db)
    recent_jobs = crud.list_jobs(db, limit=50)
    latest_zefix_job = next((j for j in recent_jobs if j.job_type == "recalculate_scores"), None)
    latest_google_job = next((j for j in recent_jobs if j.job_type == "recalculate_google_scores"), None)

    def _job_to_task(j):
        if j is None:
            return None
        stats = json.loads(j.stats_json or "{}") if j.stats_json else {}
        return {
            "type": j.job_type,
            "label": j.label,
            "message": j.message or "",
            "stats": stats,
            "error": j.error,
            "done": j.status in ("completed", "failed", "cancelled", "paused"),
        }

    scoring_task = _job_to_task(latest_zefix_job)
    google_scoring_task = _job_to_task(latest_google_job)

    active_task = task if _task_is_running(request.app.state) else None
    if active_task is None and active_jobs:
        j = active_jobs[0]
        active_task = {
            "type": j.job_type,
            "label": j.label,
            "message": j.message or j.status,
            "stats": json.loads(j.stats_json or "{}") if j.stats_json else {},
            "error": j.error,
            "done": False,
        }

    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "google_search_enabled": current.get("google_search_enabled", "true") == "true",
            "google_daily_quota": current.get("google_daily_quota", "100"),
            "zefix_industry_bonus": current.get("zefix_industry_bonus", "15"),
            "zefix_treuhand_consulting_penalty": current.get("zefix_treuhand_consulting_penalty", "15"),
            "zefix_inactive_status_penalty": current.get("zefix_inactive_status_penalty", "40"),
            "zefix_force_zero_status_terms": current.get("zefix_force_zero_status_terms", "being_cancelled"),
            "message": message,
            "error": error,
            "active_task": active_task,
            "scoring_task": scoring_task,
            "google_scoring_task": google_scoring_task,
        },
    )


@router.post("/ui/settings", include_in_schema=False)
def save_settings(
    google_search_enabled: str = Form("false"),
    google_daily_quota: str = Form("100"),
    zefix_industry_bonus: str = Form("15"),
    zefix_treuhand_consulting_penalty: str = Form("15"),
    zefix_inactive_status_penalty: str = Form("40"),
    zefix_force_zero_status_terms: str = Form("being_cancelled"),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    crud.set_setting(db, "google_search_enabled", "true" if google_search_enabled == "true" else "false")
    quota = max(1, int(google_daily_quota)) if google_daily_quota.isdigit() else 100
    crud.set_setting(db, "google_daily_quota", str(quota))

    defaults = get_default_scoring_config()
    score_fields = {
        "zefix_industry_bonus": zefix_industry_bonus,
        "zefix_treuhand_consulting_penalty": zefix_treuhand_consulting_penalty,
        "zefix_inactive_status_penalty": zefix_inactive_status_penalty,
        "zefix_force_zero_status_terms": zefix_force_zero_status_terms,
    }
    for key, value in score_fields.items():
        if key == "zefix_force_zero_status_terms":
            cleaned = value.strip() or defaults[key]
        else:
            cleaned = str(int(value)) if value.strip().lstrip("-").isdigit() else defaults[key]
        crud.set_setting(db, key, cleaned)

    return RedirectResponse(
        url=f"/ui/settings?message={quote_plus('Settings saved')}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/scoring/recalculate", include_in_schema=False)
def start_recalculate_scores(request: Request) -> RedirectResponse:
    job, err = _enqueue_job_safe(
        request,
        job_type="recalculate_scores",
        label="Recalculate Zefix scoring",
        params={},
    )
    if err:
        return RedirectResponse(
            url=f"/ui/settings?error={quote_plus(err)}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    return RedirectResponse(
        url=f"/ui/settings?message={quote_plus(f'Scoring recalculation queued (job #{job.id})')}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/scoring/recalculate-google", include_in_schema=False)
def start_recalculate_google_scores(request: Request) -> RedirectResponse:
    job, err = _enqueue_job_safe(
        request,
        job_type="recalculate_google_scores",
        label="Recalculate Google website scores",
        params={},
    )
    if err:
        return RedirectResponse(
            url=f"/ui/settings?error={quote_plus(err)}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    return RedirectResponse(
        url=f"/ui/settings?message={quote_plus(f'Google score recalculation queued (job #{job.id})')}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


# ── Collection ────────────────────────────────────────────────────────────────

def _task_is_running(app_state) -> bool:
    task = getattr(app_state, "collection_task", None)
    return task is not None and not task.get("done", False)


def _sync_active_task(app_state, *, job_type: str, label: str, message: str, stats: dict, error: str | None, done: bool) -> None:
    app_state.collection_task = {
        "type": job_type,
        "label": label,
        "started_at": time.time(),
        "message": message,
        "stats": stats,
        "error": error,
        "done": done,
    }


def _run_job(app, job_id: int) -> None:
    with SessionLocal() as db:
        job = crud.get_job(db, job_id)
        if not job:
            return

        if job.status == "cancelled" or job.cancel_requested:
            crud.mark_cancelled(db, job, message="Cancelled before start")
            crud.create_event(db, job_id=job.id, level="info", message="Job cancelled before execution started")
            return

        crud.mark_running(db, job, message="Starting…")
        crud.create_event(db, job_id=job.id, level="info", message="Job started")
        _sync_active_task(
            app.state,
            job_type=job.job_type,
            label=job.label,
            message="Starting…",
            stats={},
            error=None,
            done=False,
        )

        params = json.loads(job.params_json or "{}")
        resume_from = max(0, int(job.progress_done or 0))

        def _assert_not_cancelled() -> None:
            db.refresh(job)
            if job.cancel_requested:
                raise JobCancelledError("Cancellation requested")
            if job.pause_requested:
                raise JobPausedError("Pause requested")

        try:
            if job.job_type == "recalculate_scores":
                def _progress(done: int, total: int, stats: dict) -> None:
                    _assert_not_cancelled()
                    msg = f"Recalculated {done}/{total} companies"
                    crud.update_progress(db, job, message=msg, done=done, total=total, stats=stats)
                    crud.create_event(db, job_id=job.id, level="debug", message=msg)
                    _sync_active_task(app.state, job_type=job.job_type, label=job.label, message=msg, stats=dict(stats), error=None, done=False)

                stats = recalculate_zefix_scores(db, resume_from=resume_from, progress_cb=_progress)
                done_msg = f"Done — {stats['updated']} recalculated, {stats.get('geocoded', 0)} geocoded, {len(stats['errors'])} errors"
                if resume_from:
                    done_msg += f" (resumed from {resume_from})"

            elif job.job_type == "recalculate_google_scores":
                def _progress(done: int, total: int, stats: dict) -> None:
                    _assert_not_cancelled()
                    msg = f"Recalculated Google scores for {done}/{total} companies"
                    crud.update_progress(db, job, message=msg, done=done, total=total, stats=stats)
                    crud.create_event(db, job_id=job.id, level="debug", message=msg)
                    _sync_active_task(app.state, job_type=job.job_type, label=job.label, message=msg, stats=dict(stats), error=None, done=False)

                stats = recalculate_google_scores(db, resume_from=resume_from, progress_cb=_progress)
                done_msg = (
                    f"Done — {stats['updated']} updated, {stats['skipped']} skipped, "
                    f"{len(stats['errors'])} errors"
                )
                if resume_from:
                    done_msg += f" (resumed from {resume_from})"

            elif job.job_type == "bulk":
                def _progress(canton: str, prefix: str, created: int, updated: int) -> None:
                    _assert_not_cancelled()
                    msg = f"Canton {canton} prefix {prefix} — {created} created, {updated} updated"
                    stats_now = {"created": created, "updated": updated}
                    crud.update_progress(db, job, message=msg, stats=stats_now)
                    crud.create_event(db, job_id=job.id, level="debug", message=msg)
                    _sync_active_task(app.state, job_type=job.job_type, label=job.label, message=msg, stats=stats_now, error=None, done=False)

                stats = bulk_import_zefix(
                    db,
                    cantons=params.get("cantons"),
                    active_only=params.get("active_only", True),
                    request_delay=float(params.get("delay", 0.5)),
                    resume=True,
                    progress_cb=_progress,
                )
                done_msg = f"Done — {stats['created']} created, {stats['updated']} updated, {len(stats['errors'])} errors"

            elif job.job_type == "batch":
                def _progress(done: int, total: int, stats: dict) -> None:
                    _assert_not_cancelled()
                    msg = f"Processing {done}/{total} companies"
                    crud.update_progress(db, job, message=msg, done=done, total=total, stats=stats)
                    crud.create_event(db, job_id=job.id, level="debug", message=msg)
                    _sync_active_task(app.state, job_type=job.job_type, label=job.label, message=msg, stats=dict(stats), error=None, done=False)

                stats = run_batch_collect(
                    db,
                    limit=int(params.get("limit", 100)),
                    only_missing_website=bool(params.get("only_missing_website", True)),
                    refresh_zefix=bool(params.get("refresh_zefix", False)),
                    run_google=bool(params.get("run_google", True)),
                    resume_from=resume_from,
                    progress_cb=_progress,
                )
                done_msg = (
                    f"Done — {stats['google_enriched']} enriched, "
                    f"{stats['google_no_result']} no result, {len(stats['errors'])} errors"
                )
                if resume_from:
                    done_msg += f" (resumed from {resume_from})"

            elif job.job_type == "initial":
                def _progress(done: int, total: int, stats: dict) -> None:
                    _assert_not_cancelled()
                    msg = (
                        f"Collected {done}/{total} — {stats.get('created', 0)} created, "
                        f"{stats.get('updated', 0)} updated"
                    )
                    crud.update_progress(db, job, message=msg, done=done, total=total, stats=stats)
                    crud.create_event(db, job_id=job.id, level="debug", message=msg)
                    _sync_active_task(app.state, job_type=job.job_type, label=job.label, message=msg, stats=dict(stats), error=None, done=False)

                stats = initial_collect(
                    db,
                    names=params.get("names", []),
                    uids=params.get("uids", []),
                    canton=params.get("canton"),
                    legal_form=params.get("legal_form"),
                    active_only=bool(params.get("active_only", True)),
                    run_google=bool(params.get("run_google", True)),
                    resume_from=resume_from,
                    progress_cb=_progress,
                )
                done_msg = (
                    f"Done — {stats['created']} created, {stats['updated']} updated, "
                    f"{stats['google_enriched']} enriched, {len(stats['errors'])} errors"
                )
                if resume_from:
                    done_msg += f" (resumed from {resume_from})"

            elif job.job_type == "detail":
                def _progress(done: int, total: int, stats: dict) -> None:
                    _assert_not_cancelled()
                    msg = f"Processing {done}/{total}"
                    crud.update_progress(db, job, message=msg, done=done, total=total, stats=stats)
                    crud.create_event(db, job_id=job.id, level="debug", message=msg)
                    _sync_active_task(app.state, job_type=job.job_type, label=job.label, message=msg, stats=dict(stats), error=None, done=False)

                stats = run_zefix_detail_collect(
                    db,
                    cantons=params.get("cantons"),
                    uids=params.get("uids"),
                    score_if_missing=bool(params.get("score_if_missing", True)),
                    only_missing_details=bool(params.get("only_missing_details", False)),
                    resume_from=resume_from,
                    request_delay=float(params.get("delay", 0.3)),
                    progress_cb=_progress,
                )
                done_msg = f"Done — {stats['updated']} updated, {stats['scored']} scored, {stats.get('geocoded', 0)} geocoded, {len(stats['errors'])} errors"
                if resume_from:
                    done_msg += f" (resumed from {resume_from})"
            else:
                raise RuntimeError(f"Unsupported job type: {job.job_type}")

            crud.mark_completed(db, job, message=done_msg, stats=stats)
            crud.create_event(db, job_id=job.id, level="info", message=done_msg)
            _sync_active_task(app.state, job_type=job.job_type, label=job.label, message=done_msg, stats=dict(stats), error=None, done=True)
        except JobPausedError:
            current_stats = json.loads(job.stats_json) if job.stats_json else {}
            done_n = job.progress_done or 0
            total_n = job.progress_total
            pause_msg = f"Paused at {done_n}" + (f"/{total_n}" if total_n else "")
            crud.mark_paused(db, job, message=pause_msg, stats=current_stats)
            crud.create_event(db, job_id=job.id, level="info", message=pause_msg)
            _sync_active_task(app.state, job_type=job.job_type, label=job.label, message=pause_msg, stats=current_stats, error=None, done=True)
        except JobCancelledError:
            msg = "Cancelled by user"
            crud.mark_cancelled(db, job, message=msg)
            crud.create_event(db, job_id=job.id, level="warn", message=msg)
            _sync_active_task(app.state, job_type=job.job_type, label=job.label, message=msg, stats={}, error=None, done=True)
        except Exception as exc:  # noqa: BLE001
            err = str(exc)
            crud.mark_failed(db, job, error=err)
            crud.create_event(db, job_id=job.id, level="error", message=err)
            _sync_active_task(app.state, job_type=job.job_type, label=job.label, message="Failed", stats={}, error=err, done=True)


def _job_worker_loop(app) -> None:
    app.state.job_worker_running = True
    try:
        while True:
            with SessionLocal() as db:
                next_job = crud.get_next_queued_job(db)
                if next_job is None:
                    break
                next_id = next_job.id
            _run_job(app, next_id)
    finally:
        app.state.job_worker_running = False
        with SessionLocal() as db:
            if crud.get_next_queued_job(db) is not None:
                _ensure_job_worker(app)


def _ensure_job_worker(app) -> None:
    if getattr(app.state, "job_worker_running", False):
        return
    threading.Thread(target=_job_worker_loop, args=(app,), daemon=True).start()


def kick_job_worker(app) -> None:
    """Public wrapper used by app startup to ensure queued jobs begin processing."""
    _ensure_job_worker(app)


def _enqueue_job(request: Request, *, job_type: str, label: str, params: dict) -> object:
    with SessionLocal() as db:
        job = crud.create_job(db, job_type=job_type, label=label, params=params)
        crud.create_event(db, job_id=job.id, level="info", message="Job queued")
    _ensure_job_worker(request.app)
    return job


def _enqueue_job_safe(request: Request, *, job_type: str, label: str, params: dict) -> tuple[object | None, str | None]:
    try:
        job = _enqueue_job(request, job_type=job_type, label=label, params=params)
        return job, None
    except Exception as exc:  # noqa: BLE001
        hint = " Ensure DB migrations are up to date (alembic upgrade head)."
        return None, f"{exc}{hint}"


@router.get("/ui/jobs", response_class=HTMLResponse, include_in_schema=False)
def ui_jobs(
    request: Request,
    message: str | None = Query(None),
    error: str | None = Query(None),
    db: Session = Depends(get_db),
):
    _ensure_job_worker(request.app)
    jobs = crud.list_jobs(db, limit=100)
    events_by_job = {j.id: crud.list_events(db, job_id=j.id, limit=20) for j in jobs}
    has_active = any(j.status in ("queued", "running", "paused") for j in jobs)
    return templates.TemplateResponse(
        "jobs.html",
        {
            "request": request,
            "jobs": jobs,
            "events_by_job": events_by_job,
            "has_active": has_active,
            "message": message,
            "error": error,
        },
    )


@router.post("/ui/jobs/{job_id}/cancel", include_in_schema=False)
def cancel_job(job_id: int, request: Request, db: Session = Depends(get_db)) -> RedirectResponse:
    job = crud.get_job(db, job_id)
    if not job:
        return RedirectResponse(
            url=f"/ui/jobs?error={quote_plus('Job not found')}",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    if job.status in ("queued", "paused"):
        crud.mark_cancelled(db, job, message="Cancelled before execution")
        crud.create_event(db, job_id=job.id, level="warn", message="Job cancelled")
    elif job.status == "running":
        crud.mark_cancel_requested(db, job)
        crud.create_event(db, job_id=job.id, level="warn", message="Cancellation requested")
    else:
        return RedirectResponse(
            url=f"/ui/jobs?error={quote_plus('Only queued, running, or paused jobs can be cancelled')}",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    _ensure_job_worker(request.app)
    return RedirectResponse(
        url=f"/ui/jobs?message={quote_plus(f'Cancellation requested for job #{job_id}')}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/jobs/{job_id}/pause", include_in_schema=False)
def pause_job(job_id: int, request: Request, db: Session = Depends(get_db)) -> RedirectResponse:
    job = crud.get_job(db, job_id)
    if not job:
        return RedirectResponse(
            url=f"/ui/jobs?error={quote_plus('Job not found')}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    if job.status != "running":
        return RedirectResponse(
            url=f"/ui/jobs?error={quote_plus('Only running jobs can be paused')}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    crud.mark_pause_requested(db, job)
    crud.create_event(db, job_id=job.id, level="info", message="Pause requested")
    return RedirectResponse(
        url=f"/ui/jobs?message={quote_plus(f'Pause requested for job #{job_id}')}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/jobs/{job_id}/resume", include_in_schema=False)
def resume_job(job_id: int, request: Request, db: Session = Depends(get_db)) -> RedirectResponse:
    job = crud.get_job(db, job_id)
    if not job:
        return RedirectResponse(
            url=f"/ui/jobs?error={quote_plus('Job not found')}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    if job.status != "paused":
        return RedirectResponse(
            url=f"/ui/jobs?error={quote_plus('Only paused jobs can be resumed')}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    crud.resume_paused_job(db, job)
    crud.create_event(db, job_id=job.id, level="info", message=f"Resumed from {job.progress_done or 0}")
    _ensure_job_worker(request.app)
    return RedirectResponse(
        url=f"/ui/jobs?message={quote_plus(f'Job #{job_id} resumed')}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.get("/ui/collection", response_class=HTMLResponse, include_in_schema=False)
def ui_collection(
    request: Request,
    message: str | None = Query(None),
    error: str | None = Query(None),
    db: Session = Depends(get_db),
):
    _ensure_job_worker(request.app)
    task = getattr(request.app.state, "collection_task", None)
    incomplete_bulk = crud.get_last_incomplete_bulk(db)
    runs = crud.list_runs(db, limit=20)
    return templates.TemplateResponse(
        "collection.html",
        {
            "request": request,
            "task": task,
            "cantons": SWISS_CANTONS,
            "incomplete_bulk": incomplete_bulk,
            "runs": runs,
            "message": message,
            "error": error,
        },
    )


@router.post("/ui/collection/bulk", include_in_schema=False)
async def start_bulk(
    request: Request,
    cantons: str = Form(""),          # comma-separated, blank = all
    delay: float = Form(0.5),
    include_inactive: str = Form("false"),
    resume: str = Form("false"),
) -> RedirectResponse:
    canton_list = [c.strip().upper() for c in cantons.split(",") if c.strip()] or None
    label = f"Bulk import — cantons: {', '.join(canton_list) if canton_list else 'all 26'}"
    job, err = _enqueue_job_safe(
        request,
        job_type="bulk",
        label=label,
        params={
            "cantons": canton_list,
            "active_only": include_inactive != "true",
            "delay": delay,
            "resume": resume == "true",
        },
    )
    if err:
        return RedirectResponse(
            url=f"/ui/collection?error={quote_plus(err)}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    return RedirectResponse(
        url=f"/ui/collection?message={quote_plus(f'Queued bulk job #{job.id}')}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/collection/batch", include_in_schema=False)
async def start_batch(
    request: Request,
    limit: int = Form(100),
    all_companies: str = Form("false"),
    refresh_zefix: str = Form("false"),
) -> RedirectResponse:
    job, err = _enqueue_job_safe(
        request,
        job_type="batch",
        label=f"Batch enrichment — up to {limit} companies",
        params={
            "limit": limit,
            "only_missing_website": all_companies != "true",
            "refresh_zefix": refresh_zefix == "true",
            "run_google": True,
        },
    )
    if err:
        return RedirectResponse(
            url=f"/ui/collection?error={quote_plus(err)}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    return RedirectResponse(
        url=f"/ui/collection?message={quote_plus(f'Queued batch job #{job.id}')}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/collection/initial", include_in_schema=False)
async def start_initial(
    request: Request,
    names: str = Form(""),           # newline-separated company name search terms
    uids: str = Form(""),            # newline-separated UIDs
    canton: str = Form(""),
    legal_form: str = Form(""),
    include_inactive: str = Form("false"),
    skip_google: str = Form("false"),
) -> RedirectResponse:
    name_list = [n.strip() for n in names.splitlines() if n.strip()]
    uid_list = [u.strip() for u in uids.splitlines() if u.strip()]

    if not name_list and not uid_list:
        return RedirectResponse(
            url=f"/ui/collection?error={quote_plus('Enter at least one company name or UID')}",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    job, err = _enqueue_job_safe(
        request,
        job_type="initial",
        label=f"Specific search — {len(name_list)} name(s), {len(uid_list)} UID(s)",
        params={
            "names": name_list,
            "uids": uid_list,
            "canton": canton.strip().upper() or None,
            "legal_form": legal_form.strip() or None,
            "active_only": include_inactive != "true",
            "run_google": skip_google != "true",
        },
    )
    if err:
        return RedirectResponse(
            url=f"/ui/collection?error={quote_plus(err)}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    return RedirectResponse(
        url=f"/ui/collection?message={quote_plus(f'Queued initial search job #{job.id}')}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/collection/detail", include_in_schema=False)
async def start_detail(
    request: Request,
    cantons: str = Form(""),        # comma-separated canton codes, blank = all in DB
    uids: str = Form(""),           # newline-separated UIDs for specific companies
    delay: float = Form(0.3),
    only_missing_details: str = Form("false"),
) -> RedirectResponse:
    canton_list = [c.strip().upper() for c in cantons.split(",") if c.strip()] or None
    uid_list = [u.strip() for u in uids.splitlines() if u.strip()] or None
    missing_only = only_missing_details == "true"

    if canton_list:
        label = f"Zefix detail fetch — cantons: {', '.join(canton_list)}"
    elif uid_list:
        label = f"Zefix detail fetch — {len(uid_list)} UID(s)"
    else:
        label = "Zefix detail fetch — all matching companies"

    if missing_only:
        label += " (missing details only)"

    job, err = _enqueue_job_safe(
        request,
        job_type="detail",
        label=label,
        params={
            "cantons": canton_list,
            "uids": uid_list,
            "score_if_missing": False,
            "only_missing_details": missing_only,
            "delay": delay,
        },
    )
    if err:
        return RedirectResponse(
            url=f"/ui/collection?error={quote_plus(err)}",
            status_code=status.HTTP_303_SEE_OTHER,
        )
    return RedirectResponse(
        url=f"/ui/collection?message={quote_plus(f'Queued detail job #{job.id}')}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/collection/dismiss", include_in_schema=False)
def dismiss_task(request: Request) -> RedirectResponse:
    """Clear a finished task so the next run can start."""
    task = getattr(request.app.state, "collection_task", None)
    if task and task.get("done"):
        request.app.state.collection_task = None
    return RedirectResponse(url="/ui/collection", status_code=status.HTTP_303_SEE_OTHER)
