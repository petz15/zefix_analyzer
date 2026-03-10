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
    import_company_from_zefix_uid,
    initial_collect,
    run_batch_collect,
    run_zefix_detail_collect,
)
from app.schemas.company import CompanyUpdate
from app.schemas.note import NoteCreate, NoteUpdate

router = APIRouter(tags=["ui"])
templates = Jinja2Templates(directory="app/templates")
templates.env.filters["tojson_parse"] = lambda s: json.loads(s) if s else {}

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
    min_score: str | None = Query(None),   # kept as str to tolerate empty-string submissions
    industry: str | None = Query(None),
    tags: str | None = Query(None),
    sort: str | None = Query(None),
    page: int = Query(1, ge=1),
    message: str | None = Query(None),
    error: str | None = Query(None),
    db: Session = Depends(get_db),
):
    min_score_int: int | None = int(min_score) if min_score and min_score.strip().lstrip("-").isdigit() else None
    searched_filter = _searched_bool(google_searched)
    filter_kwargs = dict(
        name_filter=q or None,
        canton=canton or None,
        review_status=review_status or None,
        proposal_status=proposal_status or None,
        google_searched=searched_filter,
        min_score=min_score_int,
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
                        google_searched, min_score_int, sort, industry, tags)
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
            "f_min_score": min_score_int if min_score_int is not None else "",
            "f_industry": industry or "",
            "f_tags": tags or "",
            "google_search_enabled": crud.get_setting(db, "google_search_enabled", "true") == "true",
            "google_daily_quota": int(crud.get_setting(db, "google_daily_quota", "100")),
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

    old_names: list = []
    if company.old_names:
        try:
            raw_old = json.loads(company.old_names)
            if isinstance(raw_old, list):
                old_names = [str(n) for n in raw_old if n]
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
        import_company_from_zefix_uid(db, company.uid)
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
    current = crud.get_all_settings(db)
    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "google_search_enabled": current.get("google_search_enabled", "true") == "true",
            "google_daily_quota": current.get("google_daily_quota", "100"),
            "message": message,
            "error": error,
        },
    )


@router.post("/ui/settings", include_in_schema=False)
def save_settings(
    google_search_enabled: str = Form("false"),
    google_daily_quota: str = Form("100"),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    crud.set_setting(db, "google_search_enabled", "true" if google_search_enabled == "true" else "false")
    quota = max(1, int(google_daily_quota)) if google_daily_quota.isdigit() else 100
    crud.set_setting(db, "google_daily_quota", str(quota))
    return RedirectResponse(
        url=f"/ui/settings?message={quote_plus('Settings saved')}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


# ── Collection ────────────────────────────────────────────────────────────────

def _task_is_running(app_state) -> bool:
    task = getattr(app_state, "collection_task", None)
    return task is not None and not task.get("done", False)


@router.get("/ui/collection", response_class=HTMLResponse, include_in_schema=False)
def ui_collection(
    request: Request,
    message: str | None = Query(None),
    error: str | None = Query(None),
    db: Session = Depends(get_db),
):
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
    if _task_is_running(request.app.state):
        return RedirectResponse(
            url=f"/ui/collection?error={quote_plus('A collection job is already running')}",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    canton_list = [c.strip().upper() for c in cantons.split(",") if c.strip()] or None

    task: dict = {
        "type": "bulk",
        "label": f"Bulk import — cantons: {', '.join(canton_list) if canton_list else 'all 26'}",
        "started_at": time.time(),
        "message": "Starting…",
        "stats": {},
        "error": None,
        "done": False,
    }
    request.app.state.collection_task = task

    def _run() -> None:
        def progress_cb(canton: str, prefix: str, created: int, updated: int) -> None:
            task["message"] = f"Canton {canton} prefix {prefix} — {created} created, {updated} updated"
            task["stats"] = {"created": created, "updated": updated}

        try:
            with SessionLocal() as db:
                stats = bulk_import_zefix(
                    db,
                    cantons=canton_list,
                    active_only=include_inactive != "true",
                    request_delay=delay,
                    resume=resume == "true",
                    progress_cb=progress_cb,
                )
            task["stats"] = stats
            task["message"] = (
                f"Done — {stats['created']} created, {stats['updated']} updated, "
                f"{len(stats['errors'])} errors"
            )
        except Exception as exc:  # noqa: BLE001
            task["error"] = str(exc)
            task["message"] = "Failed"
        finally:
            task["done"] = True

    threading.Thread(target=_run, daemon=True).start()
    return RedirectResponse(url="/ui/collection", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/ui/collection/batch", include_in_schema=False)
async def start_batch(
    request: Request,
    limit: int = Form(100),
    skip: int = Form(0),
    all_companies: str = Form("false"),
    refresh_zefix: str = Form("false"),
    skip_google: str = Form("false"),
) -> RedirectResponse:
    if _task_is_running(request.app.state):
        return RedirectResponse(
            url=f"/ui/collection?error={quote_plus('A collection job is already running')}",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    task: dict = {
        "type": "batch",
        "label": f"Batch enrichment — up to {limit} companies",
        "started_at": time.time(),
        "message": "Starting…",
        "stats": {},
        "error": None,
        "done": False,
    }
    request.app.state.collection_task = task

    def _run() -> None:
        def progress_cb(done: int, total: int, stats: dict) -> None:
            task["message"] = f"Processing {done}/{total} companies"
            task["stats"] = dict(stats)

        try:
            with SessionLocal() as db:
                stats = run_batch_collect(
                    db,
                    limit=limit,
                    skip=skip,
                    only_missing_website=all_companies != "true",
                    refresh_zefix=refresh_zefix == "true",
                    run_google=skip_google != "true",
                    progress_cb=progress_cb,
                )
            task["stats"] = stats
            task["message"] = (
                f"Done — {stats['google_enriched']} enriched, "
                f"{stats['google_no_result']} no result, "
                f"{len(stats['errors'])} errors"
            )
        except Exception as exc:  # noqa: BLE001
            task["error"] = str(exc)
            task["message"] = "Failed"
        finally:
            task["done"] = True

    threading.Thread(target=_run, daemon=True).start()
    return RedirectResponse(url="/ui/collection", status_code=status.HTTP_303_SEE_OTHER)


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
    if _task_is_running(request.app.state):
        return RedirectResponse(
            url=f"/ui/collection?error={quote_plus('A collection job is already running')}",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    name_list = [n.strip() for n in names.splitlines() if n.strip()]
    uid_list = [u.strip() for u in uids.splitlines() if u.strip()]

    if not name_list and not uid_list:
        return RedirectResponse(
            url=f"/ui/collection?error={quote_plus('Enter at least one company name or UID')}",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    task: dict = {
        "type": "initial",
        "label": (
            f"Specific search — {len(name_list)} name(s), {len(uid_list)} UID(s)"
        ),
        "started_at": time.time(),
        "message": "Starting…",
        "stats": {},
        "error": None,
        "done": False,
    }
    request.app.state.collection_task = task

    def _run() -> None:
        try:
            with SessionLocal() as db:
                stats = initial_collect(
                    db,
                    names=name_list,
                    uids=uid_list,
                    canton=canton.strip().upper() or None,
                    legal_form=legal_form.strip() or None,
                    active_only=include_inactive != "true",
                    run_google=skip_google != "true",
                )
            task["stats"] = stats
            task["message"] = (
                f"Done — {stats['created']} created, {stats['updated']} updated, "
                f"{stats['google_enriched']} enriched, {len(stats['errors'])} errors"
            )
        except Exception as exc:  # noqa: BLE001
            task["error"] = str(exc)
            task["message"] = "Failed"
        finally:
            task["done"] = True

    threading.Thread(target=_run, daemon=True).start()
    return RedirectResponse(url="/ui/collection", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/ui/collection/detail", include_in_schema=False)
async def start_detail(
    request: Request,
    cantons: str = Form(""),        # comma-separated canton codes, blank = all in DB
    uids: str = Form(""),           # newline-separated UIDs for specific companies
    limit: int = Form(500),
    skip: int = Form(0),
    score_if_missing: str = Form("true"),
    delay: float = Form(0.3),
) -> RedirectResponse:
    if _task_is_running(request.app.state):
        return RedirectResponse(
            url=f"/ui/collection?error={quote_plus('A collection job is already running')}",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    canton_list = [c.strip().upper() for c in cantons.split(",") if c.strip()] or None
    uid_list = [u.strip() for u in uids.splitlines() if u.strip()] or None

    if canton_list:
        label = f"Zefix detail fetch — cantons: {', '.join(canton_list)}"
    elif uid_list:
        label = f"Zefix detail fetch — {len(uid_list)} UID(s)"
    else:
        label = f"Zefix detail fetch — up to {limit} companies"

    task: dict = {
        "type": "detail",
        "label": label,
        "started_at": time.time(),
        "message": "Starting…",
        "stats": {},
        "error": None,
        "done": False,
    }
    request.app.state.collection_task = task

    def _run() -> None:
        def progress_cb(done: int, total: int, stats: dict) -> None:
            task["message"] = f"Processing {done}/{total}"
            task["stats"] = dict(stats)

        try:
            with SessionLocal() as db:
                stats = run_zefix_detail_collect(
                    db,
                    cantons=canton_list,
                    uids=uid_list,
                    limit=limit,
                    skip=skip,
                    score_if_missing=score_if_missing == "true",
                    request_delay=delay,
                    progress_cb=progress_cb,
                )
            task["stats"] = stats
            task["message"] = (
                f"Done — {stats['updated']} updated, {stats['scored']} scored, "
                f"{len(stats['errors'])} errors"
            )
        except Exception as exc:  # noqa: BLE001
            task["error"] = str(exc)
            task["message"] = "Failed"
        finally:
            task["done"] = True

    threading.Thread(target=_run, daemon=True).start()
    return RedirectResponse(url="/ui/collection", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/ui/collection/dismiss", include_in_schema=False)
def dismiss_task(request: Request) -> RedirectResponse:
    """Clear a finished task so the next run can start."""
    task = getattr(request.app.state, "collection_task", None)
    if task and task.get("done"):
        request.app.state.collection_task = None
    return RedirectResponse(url="/ui/collection", status_code=status.HTTP_303_SEE_OTHER)
