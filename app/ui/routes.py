import csv
import io
import json
import logging
import threading
import time
import traceback

logger = logging.getLogger(__name__)
from urllib.parse import quote_plus, urlencode

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func
from sqlalchemy.orm import Session

from app import crud
from app.api.zefix_client import SWISS_CANTONS
from app.database import SessionLocal, get_db
from app.services.collection import (
    bulk_import_zefix,
    claude_classify_batch,
    enrich_company_website,
    geocode_and_update_company,
    import_company_from_zefix_uid,
    initial_collect,
    re_geocode_all_companies,
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
MAP_DATA_MAX_POINTS = 20000

_DEFAULT_CLAUDE_CATEGORIES = (
    "Accommodation & Hotels\n"
    "Accounting & Tax Advisory\n"
    "Advertising & Marketing Agency\n"
    "Agriculture & Farming\n"
    "Architecture & Urban Planning\n"
    "Art & Creative Studios\n"
    "Automotive Sales & Repair\n"
    "Bakery & Confectionery\n"
    "Beauty & Cosmetics\n"
    "Biotechnology\n"
    "Bookkeeping & Payroll Services\n"
    "Building Materials & Supply\n"
    "Business Consulting\n"
    "Catering & Event Catering\n"
    "Chemical Manufacturing\n"
    "Civil Engineering\n"
    "Cleaning & Facility Services\n"
    "Clothing & Fashion Retail\n"
    "Construction & General Contracting\n"
    "Courier & Delivery Services\n"
    "Craft & Artisan Production\n"
    "Data Analytics & Business Intelligence\n"
    "Dental Practice\n"
    "Design & Graphic Design\n"
    "E-Commerce & Online Retail\n"
    "Education & Training\n"
    "Electrical Engineering & Installation\n"
    "Electronic Components & Manufacturing\n"
    "Energy & Utilities\n"
    "Environmental Services\n"
    "Event Management & Organisation\n"
    "Export & International Trade\n"
    "Financial Advisory & Wealth Management\n"
    "Financial Services & Banking\n"
    "Fire & Safety Services\n"
    "Fitness & Sports\n"
    "Food & Beverage Manufacturing\n"
    "Food Import & Distribution\n"
    "Forestry & Wood Processing\n"
    "Freight & Logistics\n"
    "Funeral Services\n"
    "Furniture & Interior Design\n"
    "Garden & Landscape Services\n"
    "Gastronomy & Restaurants\n"
    "General Retail\n"
    "Geology & Surveying\n"
    "Graphic & Print Services\n"
    "Hardware & Tools Retail\n"
    "Healthcare & Medical Services\n"
    "Heating, Ventilation & Air Conditioning\n"
    "Holding Company\n"
    "Home Services & Repairs\n"
    "Hospitality & Food Service\n"
    "Human Resources & Recruitment\n"
    "HVAC & Plumbing\n"
    "Import & Wholesale Trade\n"
    "Industrial Automation\n"
    "Industrial Equipment & Machinery\n"
    "Information Technology Services\n"
    "Insurance\n"
    "Interior Architecture\n"
    "Investment & Asset Management\n"
    "Jewellery & Watches\n"
    "Journalism & Media\n"
    "Language & Translation Services\n"
    "Law & Legal Services\n"
    "Lighting & Electrical Products\n"
    "Logistics & Supply Chain\n"
    "Machine Building & Mechanical Engineering\n"
    "Management Consulting\n"
    "Manufacturing – Other\n"
    "Marine & Water Transport\n"
    "Measurement & Testing Equipment\n"
    "Medical Devices & Equipment\n"
    "Mental Health & Therapy\n"
    "Metal Processing & Metalwork\n"
    "Mobile & Telecom Services\n"
    "Music & Entertainment\n"
    "Non-Profit & Association\n"
    "Notary & Civil Law Services\n"
    "Nursing & Care Services\n"
    "Office & Business Equipment\n"
    "Optical & Precision Instruments\n"
    "Packaging & Labelling\n"
    "Painting & Surface Treatment\n"
    "Pest Control\n"
    "Pet Care & Veterinary Services\n"
    "Pharmaceutical & Drugs\n"
    "Photography & Videography\n"
    "Physical Therapy & Rehabilitation\n"
    "Plant & Equipment Rental\n"
    "Plastics & Rubber Manufacturing\n"
    "Printing & Publishing\n"
    "Process Engineering\n"
    "Project Management\n"
    "Property Management\n"
    "Public Relations\n"
    "Real Estate – Development\n"
    "Real Estate – Sales & Brokerage\n"
    "Recycling & Waste Management\n"
    "Research & Development\n"
    "Restaurant Equipment & Supplies\n"
    "Road Transport & Haulage\n"
    "Roofing & Waterproofing\n"
    "Safety & Security Services\n"
    "Scaffolding & Access Systems\n"
    "Security Technology\n"
    "Social & Community Services\n"
    "Software Development\n"
    "Solar & Renewable Energy\n"
    "Spa & Wellness\n"
    "Sports & Recreation\n"
    "Stone & Tile Work\n"
    "Storage & Warehousing\n"
    "Structural Engineering\n"
    "Sustainability & ESG Consulting\n"
    "Telecommunications Equipment\n"
    "Textile & Apparel Manufacturing\n"
    "Tourism & Travel Services\n"
    "Trade & Commerce – General\n"
    "Training & Coaching\n"
    "Translation & Interpreting\n"
    "Transport & Moving Services\n"
    "Trust & Fiduciary Services\n"
    "Vehicle Fleet Management\n"
    "Veterinary Practice\n"
    "Video & Film Production\n"
    "Web Development & Digital Agency\n"
    "Wholesale – Food & Grocery\n"
    "Wholesale – Industrial Goods\n"
    "Window & Door Manufacturing\n"
    "Wood & Joinery Work\n"
    "Other"
)


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
    tags: str | None,
    min_claude_score: int | None = None,
    tfidf_cluster: str | None = None,
    purpose_keywords: str | None = None,
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
    if min_claude_score is not None:
        p["min_claude_score"] = min_claude_score
    if sort:
        p["sort"] = sort
    if tags:
        p["tags"] = tags
    if tfidf_cluster:
        p["tfidf_cluster"] = tfidf_cluster
    if purpose_keywords:
        p["purpose_keywords"] = purpose_keywords
    return p


def _searched_bool(google_searched: str | None) -> str | None:
    """Pass google_searched string through; supports 'yes', 'no', 'no_result'."""
    return google_searched or None


_ENDPOINT_PATHS: dict[str, str] = {
    "ui_home":           "/ui",
    "ui_collection":     "/ui/collection",
    "ui_settings":       "/ui/settings",
    "ui_jobs":           "/ui/jobs",
    "ui_map":            "/ui/map",
}


def _url_for(request: Request, endpoint: str, **kwargs) -> str:
    """Build a root-relative URL for server-side redirects.

    Uses hardcoded paths to avoid request.url_for() generating absolute
    http:// URLs that break behind an HTTPS reverse proxy.
    Path params (e.g. company_id) must be passed as kwargs alongside query params.
    """
    if endpoint == "ui_company_detail":
        company_id = kwargs.pop("company_id", "")
        base = f"/ui/companies/{company_id}"
    else:
        base = _ENDPOINT_PATHS.get(endpoint, "/ui")
    clean = {k: v for k, v in kwargs.items() if v is not None and v != ""}
    return f"{base}?{urlencode(clean)}" if clean else base


@router.get("/", include_in_schema=False)
def root_redirect(request: Request) -> RedirectResponse:
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
    min_claude_score: str | None = Query(None),
    claude_category: str | None = Query(None),
    tags: str | None = Query(None),
    tfidf_cluster: str | None = Query(None),
    purpose_keywords: str | None = Query(None),
    sort: str | None = Query(None),
    page: int = Query(1, ge=1),
    message: str | None = Query(None),
    error: str | None = Query(None),
    db: Session = Depends(get_db),
):
    _ensure_job_worker(request.app)
    min_google_score_int: int | None = int(min_google_score) if min_google_score and min_google_score.strip().lstrip("-").isdigit() else None
    min_zefix_score_int: int | None = int(min_zefix_score) if min_zefix_score and min_zefix_score.strip().lstrip("-").isdigit() else None
    min_claude_score_int: int | None = int(min_claude_score) if min_claude_score and min_claude_score.strip().lstrip("-").isdigit() else None
    searched_filter = _searched_bool(google_searched)
    filter_kwargs = dict(
        name_filter=q or None,
        canton=canton or None,
        review_status=review_status or None,
        proposal_status=proposal_status or None,
        google_searched=searched_filter,
        min_google_score=min_google_score_int,
        min_zefix_score=min_zefix_score_int,
        min_claude_score=min_claude_score_int,
        claude_category=claude_category or None,
        tags=tags or None,
        tfidf_cluster=tfidf_cluster or None,
        purpose_keywords=purpose_keywords or None,
    )

    companies = crud.list_companies(db, page=page, page_size=PAGE_SIZE,
                                    sort=sort or "-updated", **filter_kwargs)
    total = crud.count_companies(db, **filter_kwargs)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    stats = crud.get_company_stats(db)

    # Build base query string (without page) for pagination links
    fp = _filter_params(q, canton, review_status, proposal_status,
                        google_searched, min_google_score_int, min_zefix_score_int,
                        sort, tags, min_claude_score_int, tfidf_cluster or None,
                        purpose_keywords or None)
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
            "f_min_claude_score": min_claude_score_int if min_claude_score_int is not None else "",
            "f_claude_category": claude_category or "",
            "f_tags": tags or "",
            "f_tfidf_cluster": tfidf_cluster or "",
            "f_purpose_keywords": purpose_keywords or "",
            "taxonomy_stats": crud.get_taxonomy_stats(db),
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
    min_claude_score: str | None = Query(None),
    min_combined_score: str | None = Query(None),
    keywords: str | None = Query(None),
    hide_cancelled: str | None = Query(None),
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
            "f_min_claude_score": min_claude_score or "",
            "f_min_combined_score": min_combined_score or "",
            "f_keywords": keywords or "",
            "f_hide_cancelled": hide_cancelled == "true",
        },
    )


from fastapi.responses import JSONResponse  # noqa: E402 (local import to avoid top-level churn)


@router.get("/api/task-status", include_in_schema=False)
def api_task_status(request: Request):
    task = getattr(request.app.state, "collection_task", None)
    if task and not task.get("done", False):
        return JSONResponse({"running": True, "label": task.get("label", ""), "message": task.get("message", "")})
    return JSONResponse({"running": False, "label": "", "message": ""})


@router.get("/api/map-data", include_in_schema=False)
def api_map_data(
    canton: str | None = Query(None),
    review_status: str | None = Query(None),
    google_searched: str | None = Query(None),
    min_google_score: int | None = Query(None),
    min_zefix_score: int | None = Query(None),
    min_claude_score: int | None = Query(None),
    min_combined_score: int | None = Query(None),
    keywords: str | None = Query(None),
    hide_cancelled: bool = Query(False),
    min_lat: float | None = Query(None),
    max_lat: float | None = Query(None),
    min_lon: float | None = Query(None),
    max_lon: float | None = Query(None),
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
        CompanyModel.claude_score,
        CompanyModel.canton,
        CompanyModel.municipality,
        CompanyModel.website_url,
        CompanyModel.review_status,
        CompanyModel.status,
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
    if min_claude_score is not None:
        query = query.filter(CompanyModel.claude_score >= min_claude_score)
    if min_combined_score is not None:
        _combined_expr = (
            func.coalesce(CompanyModel.claude_score * 0.70, 0.0)
            + func.coalesce(CompanyModel.website_match_score * 0.20, 0.0)
            + func.coalesce(CompanyModel.zefix_score * 0.10, 0.0)
        )
        query = query.filter(_combined_expr >= min_combined_score)
    if keywords:
        kw_terms = [t.strip() for t in keywords.split(",") if t.strip()]
        if kw_terms:
            from sqlalchemy import or_
            query = query.filter(or_(*(
                or_(
                    CompanyModel.purpose_keywords.ilike(f"%{kw}%"),
                    CompanyModel.tfidf_cluster.ilike(f"%{kw}%"),
                )
                for kw in kw_terms
            )))
    if hide_cancelled:
        _cancelled_terms = ["being_cancelled", "dissolved", "gelöscht", "radiation", "liquidation"]
        from sqlalchemy import or_
        query = query.filter(~or_(*(
            CompanyModel.status.ilike(f"%{t}%") for t in _cancelled_terms
        )))
    if None not in (min_lat, max_lat):
        query = query.filter(CompanyModel.lat >= min_lat, CompanyModel.lat <= max_lat)
    if None not in (min_lon, max_lon):
        query = query.filter(CompanyModel.lon >= min_lon, CompanyModel.lon <= max_lon)

    # Keep the endpoint responsive when users are zoomed out over very large areas.
    rows = query.limit(MAP_DATA_MAX_POINTS + 1).all()
    truncated = len(rows) > MAP_DATA_MAX_POINTS
    if truncated:
        rows = rows[:MAP_DATA_MAX_POINTS]

    features = [
        {
            "id": r.id,
            "name": r.name,
            "lat": r.lat,
            "lon": r.lon,
            "google_score": r.website_match_score,
            "zefix_score": r.zefix_score,
            "claude_score": r.claude_score,
            "canton": r.canton,
            "municipality": r.municipality,
            "website": r.website_url,
            "review": r.review_status,
            "status": r.status,
        }
        for r in rows
    ]
    return JSONResponse(
        {
            "count": len(features),
            "features": features,
            "truncated": truncated,
            "max_points": MAP_DATA_MAX_POINTS,
        }
    )


@router.get("/ui/export.csv", include_in_schema=False)
def export_csv(
    q: str | None = Query(None),
    canton: str | None = Query(None),
    review_status: str | None = Query(None),
    proposal_status: str | None = Query(None),
    google_searched: str | None = Query(None),
    min_google_score: int | None = Query(None),
    min_zefix_score: int | None = Query(None),
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
        tags=tags or None,
    )

    def _generate():
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "uid", "name", "legal_form", "status", "municipality", "canton",
            "website_url", "website_match_score", "review_status", "proposal_status",
            "contact_name", "contact_email", "contact_phone", "tags",
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
                c.tags or "",
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
    # Record one audit entry per company
    for cid in company_ids:
        crud.create_audit_entry(
            db,
            company_id=cid,
            user_id=None,
            field=field,
            old_value=None,  # old value not captured in bulk for perf
            new_value=str(value) if value else None,
        )
    label = value or "cleared"
    return RedirectResponse(
        url=f"{back}&message={quote_plus(f'{len(company_ids)} companies set to {label}')}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.get("/ui/companies/{company_id}", response_class=HTMLResponse, include_in_schema=False)
def ui_company_detail(
    company_id: int,
    request: Request,
    back: str | None = Query(None),
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

    audit_log = crud.list_audit_for_company(db, company_id, limit=30)

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
            "audit_log": audit_log,
            "back_url": back or "/ui",
            "message": message,
            "error": error,
        },
    )


@router.post("/ui/companies/{company_id}/zefix-refresh", include_in_schema=False)
def zefix_refresh_company(company_id: int, request: Request, db: Session = Depends(get_db)) -> RedirectResponse:
    company = crud.get_company(db, company_id)
    if not company:
        return RedirectResponse(url=_url_for(request, "ui_home", error="Company not found"), status_code=status.HTTP_303_SEE_OTHER)

    try:
        updated, _ = import_company_from_zefix_uid(db, company.uid)
        geocode_and_update_company(db, updated)
    except Exception as exc:  # noqa: BLE001
        return RedirectResponse(
            url=_url_for(request, "ui_company_detail", company_id=company_id, error=str(exc)),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    return RedirectResponse(
        url=_url_for(request, "ui_company_detail", company_id=company_id, message="Zefix data refreshed"),
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/companies/{company_id}/google-search", include_in_schema=False)
def google_search_for_company(company_id: int, request: Request, db: Session = Depends(get_db)) -> RedirectResponse:
    if crud.get_setting(db, "google_search_enabled", "true") != "true":
        return RedirectResponse(
            url=_url_for(request, "ui_company_detail", company_id=company_id, error="Google Search is disabled (GOOGLE_SEARCH_ENABLED=false)"),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    company = crud.get_company(db, company_id)
    if not company:
        return RedirectResponse(url=_url_for(request, "ui_home", error="Company not found"), status_code=status.HTTP_303_SEE_OTHER)

    try:
        enriched, _ = enrich_company_website(db, company)
    except Exception as exc:  # noqa: BLE001
        return RedirectResponse(
            url=_url_for(request, "ui_company_detail", company_id=company_id, error=str(exc)),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    msg = "Google search complete — results scored and saved" if enriched else "No search results returned"
    return RedirectResponse(
        url=_url_for(request, "ui_company_detail", company_id=company_id, message=msg),
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/companies/{company_id}/edit", include_in_schema=False)
def edit_company(
    company_id: int,
    request: Request,
    website_url: str = Form(""),
    review_status: str = Form(""),
    proposal_status: str = Form(""),
    contact_name: str = Form(""),
    contact_email: str = Form(""),
    contact_phone: str = Form(""),
    tags: str = Form(""),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    company = crud.get_company(db, company_id)
    if not company:
        return RedirectResponse(url=_url_for(request, "ui_home", error="Company not found"), status_code=status.HTTP_303_SEE_OTHER)

    new_values = dict(
        website_url=website_url.strip() or None,
        review_status=review_status or None,
        proposal_status=proposal_status or None,
        contact_name=contact_name.strip() or None,
        contact_email=contact_email.strip() or None,
        contact_phone=contact_phone.strip() or None,
        tags=tags.strip() or None,
    )
    old_values = {f: getattr(company, f) for f in new_values}

    crud.update_company(db, company, CompanyUpdate(**new_values))
    crud.record_company_changes(
        db,
        company_id=company_id,
        user_id=None,
        old_values=old_values,
        new_values=new_values,
    )
    return RedirectResponse(
        url=_url_for(request, "ui_company_detail", company_id=company_id, message="Company updated"),
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/companies/{company_id}/quick-status", include_in_schema=False)
def quick_status(
    company_id: int,
    review_status: str | None = Form(None),
    proposal_status: str | None = Form(None),
    db: Session = Depends(get_db),
):
    company = crud.get_company(db, company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    new_values: dict = {}
    if review_status is not None:
        new_values["review_status"] = review_status or None
    if proposal_status is not None:
        new_values["proposal_status"] = proposal_status or None

    if new_values:
        old_values = {f: getattr(company, f) for f in new_values}
        crud.update_company(db, company, CompanyUpdate(**new_values))
        crud.record_company_changes(
            db,
            company_id=company_id,
            user_id=None,
            old_values=old_values,
            new_values=new_values,
        )
    from fastapi.responses import Response
    return Response(status_code=204)


@router.post("/ui/companies/{company_id}/set-website", include_in_schema=False)
def set_website(
    company_id: int,
    request: Request,
    website_url: str = Form(...),
    website_match_score: int = Form(0),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    company = crud.get_company(db, company_id)
    if not company:
        return RedirectResponse(url=_url_for(request, "ui_home", error="Company not found"), status_code=status.HTTP_303_SEE_OTHER)

    crud.update_company(
        db,
        company,
        CompanyUpdate(website_url=website_url.strip() or None, website_match_score=website_match_score),
    )
    return RedirectResponse(
        url=_url_for(request, "ui_company_detail", company_id=company_id, message="Website updated"),
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/companies/{company_id}/notes", include_in_schema=False)
def create_note(
    company_id: int,
    request: Request,
    content: str = Form(...),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    company = crud.get_company(db, company_id)
    if not company:
        return RedirectResponse(url=_url_for(request, "ui_home", error="Company not found"), status_code=status.HTTP_303_SEE_OTHER)

    content_clean = content.strip()
    if not content_clean:
        return RedirectResponse(
            url=_url_for(request, "ui_company_detail", company_id=company_id, error="Note content cannot be empty"),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    crud.create_note(db, company_id, NoteCreate(content=content_clean))
    return RedirectResponse(
        url=_url_for(request, "ui_company_detail", company_id=company_id, message="Note added"),
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/companies/{company_id}/notes/{note_id}/edit", include_in_schema=False)
def edit_note(
    company_id: int,
    note_id: int,
    request: Request,
    content: str = Form(...),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    note = crud.get_note(db, note_id)
    if not note or note.company_id != company_id:
        return RedirectResponse(
            url=_url_for(request, "ui_company_detail", company_id=company_id, error="Note not found"),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    content_clean = content.strip()
    if not content_clean:
        return RedirectResponse(
            url=_url_for(request, "ui_company_detail", company_id=company_id, error="Note content cannot be empty"),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    crud.update_note(db, note, NoteUpdate(content=content_clean))
    return RedirectResponse(
        url=_url_for(request, "ui_company_detail", company_id=company_id, message="Note updated"),
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/companies/{company_id}/notes/{note_id}/delete", include_in_schema=False)
def delete_note(company_id: int, note_id: int, request: Request, db: Session = Depends(get_db)) -> RedirectResponse:
    note = crud.get_note(db, note_id)
    if not note or note.company_id != company_id:
        return RedirectResponse(
            url=_url_for(request, "ui_company_detail", company_id=company_id, error="Note not found"),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    crud.delete_note(db, note)
    return RedirectResponse(
        url=_url_for(request, "ui_company_detail", company_id=company_id, message="Note deleted"),
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
            "scoring_target_clusters": current.get("scoring_target_clusters", ""),
            "scoring_cluster_hit_points": current.get("scoring_cluster_hit_points", "10"),
            "scoring_exclude_clusters": current.get("scoring_exclude_clusters", ""),
            "scoring_cluster_exclude_points": current.get("scoring_cluster_exclude_points", "10"),
            "scoring_target_keywords": current.get("scoring_target_keywords", ""),
            "scoring_keyword_hit_points": current.get("scoring_keyword_hit_points", "10"),
            "scoring_exclude_keywords": current.get("scoring_exclude_keywords", ""),
            "scoring_keyword_exclude_points": current.get("scoring_keyword_exclude_points", "10"),
            "scoring_origin_lat": current.get("scoring_origin_lat", "46.9266"),
            "scoring_origin_lon": current.get("scoring_origin_lon", "7.4817"),
            "scoring_dist_15km": current.get("scoring_dist_15km", "20"),
            "scoring_dist_40km": current.get("scoring_dist_40km", "10"),
            "scoring_dist_80km": current.get("scoring_dist_80km", "5"),
            "scoring_dist_130km": current.get("scoring_dist_130km", "0"),
            "scoring_dist_far": current.get("scoring_dist_far", "-5"),
            "scoring_legal_form_scores": current.get("scoring_legal_form_scores", "gmbh:20,sarl:20,sàrl:20,einzelfirma:15,eg:15,kg:10,og:8,ag:8,sa:8,stiftung:3,verein:2"),
            "scoring_legal_form_default": current.get("scoring_legal_form_default", "5"),
            "scoring_cancelled_score": current.get("scoring_cancelled_score", "5"),
            "anthropic_api_key": current.get("anthropic_api_key", ""),
            "claude_target_description": current.get("claude_target_description", ""),
            "claude_classify_prompt": current.get("claude_classify_prompt", ""),
            "claude_classify_categories": current.get("claude_classify_categories", "") or _DEFAULT_CLAUDE_CATEGORIES,
            "scoring_claude_max_purpose_chars": current.get("scoring_claude_max_purpose_chars", "800"),
            "boilerplate_patterns": crud.list_boilerplate_patterns(db),
            "message": message,
            "error": error,
            "active_task": active_task,
            "scoring_task": scoring_task,
            "google_scoring_task": google_scoring_task,
        },
    )


@router.post("/ui/settings", include_in_schema=False)
def save_settings(
    request: Request,
    google_search_enabled: str = Form("false"),
    google_daily_quota: str = Form("100"),
    scoring_target_clusters: str = Form(""),
    scoring_cluster_hit_points: str = Form("10"),
    scoring_exclude_clusters: str = Form(""),
    scoring_cluster_exclude_points: str = Form("10"),
    scoring_target_keywords: str = Form(""),
    scoring_keyword_hit_points: str = Form("10"),
    scoring_exclude_keywords: str = Form(""),
    scoring_keyword_exclude_points: str = Form("10"),
    scoring_origin_lat: str = Form("46.9266"),
    scoring_origin_lon: str = Form("7.4817"),
    scoring_dist_15km: str = Form("20"),
    scoring_dist_40km: str = Form("10"),
    scoring_dist_80km: str = Form("5"),
    scoring_dist_130km: str = Form("0"),
    scoring_dist_far: str = Form("-5"),
    scoring_legal_form_scores: str = Form("gmbh:20,sarl:20,sàrl:20,einzelfirma:15,eg:15,kg:10,og:8,ag:8,sa:8,stiftung:3,verein:2"),
    scoring_legal_form_default: str = Form("5"),
    scoring_cancelled_score: str = Form("5"),
    anthropic_api_key: str = Form(""),
    claude_target_description: str = Form(""),
    claude_classify_prompt: str = Form(""),
    scoring_claude_max_purpose_chars: str = Form("800"),
    claude_classify_categories: str = Form(""),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    crud.set_setting(db, "google_search_enabled", "true" if google_search_enabled == "true" else "false")
    quota = max(1, int(google_daily_quota)) if google_daily_quota.isdigit() else 100
    crud.set_setting(db, "google_daily_quota", str(quota))

    defaults = get_default_scoring_config()
    # Text fields saved as-is; numeric fields validated (fall back to default on bad input)
    _text_fields = {
        "scoring_target_clusters": scoring_target_clusters,
        "scoring_exclude_clusters": scoring_exclude_clusters,
        "scoring_target_keywords": scoring_target_keywords,
        "scoring_exclude_keywords": scoring_exclude_keywords,
        "scoring_legal_form_scores": scoring_legal_form_scores,
    }
    for key, value in _text_fields.items():
        crud.set_setting(db, key, value.strip())

    _numeric_fields = {
        "scoring_cluster_hit_points": scoring_cluster_hit_points,
        "scoring_cluster_exclude_points": scoring_cluster_exclude_points,
        "scoring_keyword_hit_points": scoring_keyword_hit_points,
        "scoring_keyword_exclude_points": scoring_keyword_exclude_points,
        "scoring_origin_lat": scoring_origin_lat,
        "scoring_origin_lon": scoring_origin_lon,
        "scoring_dist_15km": scoring_dist_15km,
        "scoring_dist_40km": scoring_dist_40km,
        "scoring_dist_80km": scoring_dist_80km,
        "scoring_dist_130km": scoring_dist_130km,
        "scoring_dist_far": scoring_dist_far,
        "scoring_legal_form_default": scoring_legal_form_default,
        "scoring_cancelled_score": scoring_cancelled_score,
    }
    for key, value in _numeric_fields.items():
        v = value.strip()
        try:
            float(v)  # accept int or float (lat/lon)
            crud.set_setting(db, key, v)
        except ValueError:
            crud.set_setting(db, key, defaults[key])

    # Free-text / API settings
    crud.set_setting(db, "anthropic_api_key", anthropic_api_key.strip())
    crud.set_setting(db, "claude_target_description", claude_target_description.strip())
    crud.set_setting(db, "claude_classify_prompt", claude_classify_prompt.strip())
    crud.set_setting(db, "claude_classify_categories", claude_classify_categories.strip())
    try:
        crud.set_setting(db, "scoring_claude_max_purpose_chars", str(int(scoring_claude_max_purpose_chars.strip())))
    except (ValueError, AttributeError):
        crud.set_setting(db, "scoring_claude_max_purpose_chars", "800")

    return RedirectResponse(
        url=_url_for(request, "ui_settings", message="Settings saved"),
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
            url=_url_for(request, "ui_settings", error=err),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    return RedirectResponse(
        url=_url_for(request, "ui_settings", message=f"Scoring recalculation queued (job #{job.id})"),
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
            url=_url_for(request, "ui_settings", error=err),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    return RedirectResponse(
        url=_url_for(request, "ui_settings", message=f"Google score recalculation queued (job #{job.id})"),
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/scoring/re-geocode", include_in_schema=False)
def start_re_geocode(request: Request, db: Session = Depends(get_db)) -> RedirectResponse:
    # Reset the done flag so the job runs even if it ran before
    crud.set_setting(db, "geocoding_building_level_done", "false")
    job, err = _enqueue_job_safe(
        request,
        job_type="re_geocode",
        label="Re-geocode all companies (building-level)",
        params={},
    )
    if err:
        return RedirectResponse(
            url=_url_for(request, "ui_settings", error=err),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    return RedirectResponse(
        url=_url_for(request, "ui_settings", message=f"Re-geocode job queued (job #{job.id})"),
        status_code=status.HTTP_303_SEE_OTHER,
    )


# ── Classification batch jobs ─────────────────────────────────────────────────

def _parse_optional_int(val: str | None) -> int | None:
    if val and val.strip().lstrip("-").isdigit():
        return int(val.strip())
    return None


@router.post("/ui/classify/hdbscan", include_in_schema=False)
def start_hdbscan_cluster(
    request: Request,
    min_cluster_size: str = Form("75"),
    min_samples: str = Form("10"),
    top_terms: str = Form("7"),
) -> RedirectResponse:
    params: dict = {
        "min_cluster_size": _parse_optional_int(min_cluster_size) or 75,
        "min_samples": _parse_optional_int(min_samples) or 10,
        "top_terms": _parse_optional_int(top_terms) or 7,
    }
    label = f"HDBSCAN clustering (min_cluster={params['min_cluster_size']})"
    job, err = _enqueue_job_safe(request, job_type="hdbscan_cluster", label=label, params=params)
    if err:
        return RedirectResponse(url=_url_for(request, "ui_settings", error=err), status_code=status.HTTP_303_SEE_OTHER)
    return RedirectResponse(url=_url_for(request, "ui_settings", message=f"HDBSCAN clustering queued (job #{job.id})"), status_code=status.HTTP_303_SEE_OTHER)


@router.post("/ui/classify/cluster-analysis", include_in_schema=False)
def start_cluster_analysis(
    request: Request,
    top_n_clusters: str = Form("20"),
    top_n_terms: str = Form("10"),
) -> RedirectResponse:
    params: dict = {
        "top_n_clusters": _parse_optional_int(top_n_clusters) or 20,
        "top_n_terms": _parse_optional_int(top_n_terms) or 10,
    }
    label = f"Cross-cluster analysis (top {params['top_n_clusters']} clusters)"
    job, err = _enqueue_job_safe(request, job_type="cluster_analysis", label=label, params=params)
    if err:
        return RedirectResponse(url=_url_for(request, "ui_settings", error=err), status_code=status.HTTP_303_SEE_OTHER)
    return RedirectResponse(url=_url_for(request, "ui_settings", message=f"Analysis queued (job #{job.id})"), status_code=status.HTTP_303_SEE_OTHER)


@router.post("/ui/classify/recompute-keywords", include_in_schema=False)
def start_recompute_keywords(
    request: Request,
    top_keywords_per_company: str = Form("10"),
    canton: str = Form(""),
    limit: str = Form(""),
) -> RedirectResponse:
    params: dict = {
        "top_keywords_per_company": _parse_optional_int(top_keywords_per_company) or 10,
        "canton": canton.strip() or None,
        "limit": _parse_optional_int(limit),
    }
    job, err = _enqueue_job_safe(request, job_type="recompute_keywords", label="Recompute purpose_keywords", params=params)
    if err:
        return RedirectResponse(url=_url_for(request, "ui_settings", error=err), status_code=status.HTTP_303_SEE_OTHER)
    return RedirectResponse(url=_url_for(request, "ui_settings", message=f"Keyword recompute queued (job #{job.id})"), status_code=status.HTTP_303_SEE_OTHER)


@router.post("/ui/classify/claude", include_in_schema=False)
def start_claude_classify(
    request: Request,
    canton: str = Form(""),
    min_zefix_score: str = Form(""),
    max_zefix_score: str = Form(""),
    min_google_score: str = Form(""),
    purpose_keywords: str = Form(""),
    rerun_classified: str = Form("false"),
    auto_filter_keywords: str = Form("false"),
    use_fixed_categories: str = Form("false"),
    limit: str = Form("500"),
    system_prompt: str = Form(""),
    use_batch_api: str = Form("false"),
    companies_per_message: str = Form("1"),
) -> RedirectResponse:
    params: dict = {"limit": _parse_optional_int(limit) or 500}
    if canton.strip():
        params["canton"] = canton.strip()
    v = _parse_optional_int(min_zefix_score)
    if v is not None:
        params["min_zefix_score"] = v
    v2 = _parse_optional_int(max_zefix_score)
    if v2 is not None:
        params["max_zefix_score"] = v2
    v3 = _parse_optional_int(min_google_score)
    if v3 is not None:
        params["min_google_score"] = v3
    if purpose_keywords.strip():
        params["purpose_keywords"] = purpose_keywords.strip()
    if rerun_classified == "true":
        params["rerun_classified"] = True
    if auto_filter_keywords == "true":
        params["auto_filter_keywords"] = True
    if use_fixed_categories == "true":
        params["use_fixed_categories"] = True
    if system_prompt.strip():
        params["system_prompt"] = system_prompt.strip()
    if use_batch_api == "true":
        params["use_batch_api"] = True
    cpm = _parse_optional_int(companies_per_message)
    if cpm and cpm > 1:
        params["companies_per_message"] = cpm

    label = f"Claude classify ({params['limit']} companies)" + (" [batch]" if params.get("use_batch_api") else "")
    job, err = _enqueue_job_safe(request, job_type="claude_classify", label=label, params=params)
    if err:
        return RedirectResponse(url=_url_for(request, "ui_settings", error=err), status_code=status.HTTP_303_SEE_OTHER)
    return RedirectResponse(url=_url_for(request, "ui_settings", message=f"Claude classification queued (job #{job.id})"), status_code=status.HTTP_303_SEE_OTHER)


# ── Boilerplate pattern management ────────────────────────────────────────────

@router.post("/ui/boilerplate/add", include_in_schema=False)
def boilerplate_add(
    request: Request,
    pattern: str = Form(""),
    description: str = Form(""),
    example: str = Form(""),
    db: Session = Depends(get_db),
) -> RedirectResponse:
    import re as _re
    pattern = pattern.strip()
    if not pattern:
        return RedirectResponse(url=_url_for(request, "ui_settings", error="Pattern cannot be empty"), status_code=status.HTTP_303_SEE_OTHER)
    try:
        _re.compile(pattern, _re.IGNORECASE)
    except _re.error as exc:
        return RedirectResponse(url=_url_for(request, "ui_settings", error=f"Invalid regex: {exc}"), status_code=status.HTTP_303_SEE_OTHER)
    crud.create_boilerplate_pattern(
        db,
        pattern=pattern,
        description=description.strip() or None,
        example=example.strip() or None,
        active=True,
    )
    return RedirectResponse(url=_url_for(request, "ui_settings", message="Boilerplate pattern added"), status_code=status.HTTP_303_SEE_OTHER)


@router.post("/ui/boilerplate/{pattern_id}/toggle", include_in_schema=False)
def boilerplate_toggle(request: Request, pattern_id: int, db: Session = Depends(get_db)) -> RedirectResponse:
    row = crud.get_boilerplate_pattern(db, pattern_id)
    if not row:
        return RedirectResponse(url=_url_for(request, "ui_settings", error="Pattern not found"), status_code=status.HTTP_303_SEE_OTHER)
    crud.update_boilerplate_pattern(db, row, active=not row.active)
    state = "enabled" if row.active else "disabled"
    return RedirectResponse(url=_url_for(request, "ui_settings", message=f"Pattern {state}"), status_code=status.HTTP_303_SEE_OTHER)


@router.post("/ui/boilerplate/{pattern_id}/delete", include_in_schema=False)
def boilerplate_delete(request: Request, pattern_id: int, db: Session = Depends(get_db)) -> RedirectResponse:
    row = crud.get_boilerplate_pattern(db, pattern_id)
    if not row:
        return RedirectResponse(url=_url_for(request, "ui_settings", error="Pattern not found"), status_code=status.HTTP_303_SEE_OTHER)
    crud.delete_boilerplate_pattern(db, row)
    return RedirectResponse(url=_url_for(request, "ui_settings", message="Pattern deleted"), status_code=status.HTTP_303_SEE_OTHER)


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
            if job.job_type == "re_geocode":
                def _progress(done: int, total: int, stats: dict) -> None:
                    _assert_not_cancelled()
                    msg = f"Geocoded {done}/{total} — {stats['geocoded']} updated, {stats['failed']} no match"
                    crud.update_progress(db, job, message=msg, done=done, total=total, stats=stats)
                    crud.create_event(db, job_id=job.id, level="debug", message=msg)
                    _sync_active_task(app.state, job_type=job.job_type, label=job.label, message=msg, stats=dict(stats), error=None, done=False)

                stats = re_geocode_all_companies(db, resume_from=resume_from, progress_cb=_progress)
                done_msg = (
                    f"Done — {stats['geocoded']} geocoded, {stats['failed']} no match, "
                    f"{len(stats['errors'])} errors"
                )
                if resume_from:
                    done_msg += f" (resumed from {resume_from})"
                # Mark upgrade complete so startup doesn't re-enqueue
                crud.set_setting(db, "geocoding_building_level_done", "true")

            elif job.job_type == "recalculate_scores":
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
                    canton=params.get("canton"),
                    min_zefix_score=params.get("min_zefix_score"),
                    min_claude_score=params.get("min_claude_score"),
                    purpose_keywords=params.get("purpose_keywords"),
                    tfidf_cluster=params.get("tfidf_cluster"),
                    review_status=params.get("review_status"),
                )
                done_msg = (
                    f"Done — {stats['google_enriched']} enriched, "
                    f"{stats['google_no_result']} no result, {len(stats['errors'])} errors"
                )
                if stats.get("warnings"):
                    done_msg += f", {len(stats['warnings'])} warning(s)"
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
            elif job.job_type == "hdbscan_cluster":
                from app.services.cluster_pipeline import PipelineConfig, run_pipeline

                def _progress(done: int, total: int, stats: dict) -> None:
                    _assert_not_cancelled()
                    step = stats.get("step", "clustering")
                    msg = f"[{step}] {done}/{total} — {stats.get('classified', 0)} clustered, {stats.get('noise', 0)} noise"
                    crud.update_progress(db, job, message=msg, done=done, total=total, stats=stats)
                    crud.create_event(db, job_id=job.id, level="debug", message=msg)
                    _sync_active_task(app.state, job_type=job.job_type, label=job.label, message=msg, stats=dict(stats), error=None, done=False)

                cfg = PipelineConfig(
                    n_clusters=int(params.get("n_clusters", 150)),
                    max_clusters_per_company=int(params.get("max_clusters_per_company", 7)),
                    min_similarity=float(params.get("min_similarity", 0.10)),
                    n_components=int(params.get("n_components", 50)),
                    top_terms_per_cluster=int(params.get("top_terms", 5)),
                    top_keywords_per_company=int(params.get("top_keywords_per_company", 10)),
                )
                stats = run_pipeline(
                    db, cfg,
                    canton=params.get("canton") or None,
                    min_zefix_score=int(params["min_zefix_score"]) if params.get("min_zefix_score") else None,
                    max_zefix_score=int(params["max_zefix_score"]) if params.get("max_zefix_score") else None,
                    limit=int(params["limit"]) if params.get("limit") else None,
                    use_keywords=bool(params.get("use_keywords", False)),
                    progress_cb=_progress,
                )
                n_c = stats.get("n_clusters", 0)
                classified = stats.get("classified", 0)
                noise = stats.get("noise", 0)
                done_msg = f"Done — {n_c} clusters, {classified} companies labelled, {noise} noise"

            elif job.job_type == "recompute_keywords":
                from app.services.cluster_pipeline import PipelineConfig, recompute_keywords

                def _progress(done: int, total: int, stats: dict) -> None:
                    _assert_not_cancelled()
                    msg = f"[{stats.get('step', 'keywords')}] {done}/{total} — {stats.get('updated', 0)} updated"
                    crud.update_progress(db, job, message=msg, done=done, total=total, stats=stats)
                    _sync_active_task(app.state, job_type=job.job_type, label=job.label, message=msg, stats=dict(stats), error=None, done=False)

                cfg = PipelineConfig(
                    top_keywords_per_company=int(params.get("top_keywords_per_company", 10)),
                )
                stats = recompute_keywords(
                    db, cfg,
                    canton=params.get("canton") or None,
                    limit=int(params["limit"]) if params.get("limit") else None,
                    progress_cb=_progress,
                )
                done_msg = f"Done — {stats['updated']} keywords updated, {stats['skipped']} skipped"

            elif job.job_type == "cluster_analysis":
                from app.services.cluster_pipeline import PipelineConfig, analyze_cross_cluster_terms

                cfg = PipelineConfig(
                    analysis_top_clusters=int(params.get("top_n_clusters", 20)),
                    analysis_top_terms=int(params.get("top_n_terms", 10)),
                )
                analyze_cross_cluster_terms(db, cfg)
                stats = {"errors": []}
                done_msg = "Cross-cluster analysis written — download at /static/cluster_analysis.txt"

            elif job.job_type == "claude_classify":
                from app.config import settings as app_settings

                def _progress(done: int, total: int, stats: dict) -> None:
                    _assert_not_cancelled()
                    tokens = stats.get("input_tokens", 0) + stats.get("output_tokens", 0)
                    batch_id = stats.get("batch_id", "")
                    batch_hint = f" · batch {batch_id}" if batch_id and done == 0 else ""
                    msg = f"Classified {done}/{total} — {stats['classified']} scored, ~{tokens} tokens used{batch_hint}"
                    crud.update_progress(db, job, message=msg, done=done, total=total, stats=stats)
                    crud.create_event(db, job_id=job.id, level="debug", message=msg)
                    _sync_active_task(app.state, job_type=job.job_type, label=job.label, message=msg, stats=dict(stats), error=None, done=False)

                stats = claude_classify_batch(
                    db,
                    canton=params.get("canton") or None,
                    min_zefix_score=params.get("min_zefix_score"),
                    max_zefix_score=params.get("max_zefix_score"),
                    min_google_score=params.get("min_google_score"),
                    purpose_keywords=params.get("purpose_keywords") or None,
                    rerun_classified=bool(params.get("rerun_classified", False)),
                    auto_filter_keywords=bool(params.get("auto_filter_keywords", False)),
                    use_fixed_categories=bool(params.get("use_fixed_categories", False)),
                    limit=int(params.get("limit", 500)),
                    system_prompt=params.get("system_prompt") or None,
                    target_description=crud.get_setting(db, "claude_target_description", "") or None,
                    api_key=crud.get_setting(db, "anthropic_api_key", "") or app_settings.anthropic_api_key,
                    resume_from=resume_from,
                    use_batch_api=bool(params.get("use_batch_api", False)),
                    companies_per_message=int(params.get("companies_per_message", 1)),
                    progress_cb=_progress,
                )
                tokens = stats.get("input_tokens", 0) + stats.get("output_tokens", 0)
                done_msg = f"Done — {stats['classified']} classified, {stats['skipped']} skipped, ~{tokens} tokens, {len(stats['errors'])} errors"

            else:
                raise RuntimeError(f"Unsupported job type: {job.job_type}")

            crud.mark_completed(db, job, message=done_msg, stats=stats)
            crud.create_event(db, job_id=job.id, level="info", message=done_msg)
            for _w in (stats.get("warnings") or [])[:10]:
                crud.create_event(db, job_id=job.id, level="warn", message=str(_w))
            for _err in (stats.get("errors") or [])[:50]:
                crud.create_event(db, job_id=job.id, level="warn", message=str(_err))
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
            err = traceback.format_exc()
            # Roll back any failed transaction so the session is usable again.
            # Without this, accessing job.id triggers a lazy load on a session
            # that is in PendingRollbackError, causing a second crash that
            # silences the original error in the UI.
            try:
                db.rollback()
            except Exception:  # noqa: BLE001
                pass
            logger.error("Job %s (%s) failed:\n%s", job.id, job.job_type, err)
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
    events_by_job = {j.id: crud.list_events(db, job_id=j.id, limit=60, exclude_debug=True) for j in jobs}
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


@router.get("/ui/jobs/stream", include_in_schema=False)
def ui_jobs_stream(db: Session = Depends(get_db)):
    def event_generator():
        while True:
            active = crud.list_active_jobs(db)
            if not active:
                yield "data: done\n\n"
                return
            yield "data: update\n\n"
            time.sleep(2)
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/ui/jobs/partial", response_class=HTMLResponse, include_in_schema=False)
def ui_jobs_partial(request: Request, db: Session = Depends(get_db)):
    jobs = crud.list_jobs(db, limit=100)
    events_by_job = {j.id: crud.list_events(db, job_id=j.id, limit=60, exclude_debug=True) for j in jobs}
    return templates.TemplateResponse(
        "jobs_table.html",
        {"request": request, "jobs": jobs, "events_by_job": events_by_job},
    )


@router.post("/ui/jobs/{job_id}/cancel", include_in_schema=False)
def cancel_job(job_id: int, request: Request, db: Session = Depends(get_db)) -> RedirectResponse:
    job = crud.get_job(db, job_id)
    if not job:
        return RedirectResponse(
            url=_url_for(request, "ui_jobs", error="Job not found"),
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
            url=_url_for(request, "ui_jobs", error="Only queued, running, or paused jobs can be cancelled"),
            status_code=status.HTTP_303_SEE_OTHER,
        )

    _ensure_job_worker(request.app)
    return RedirectResponse(
        url=_url_for(request, "ui_jobs", message=f"Cancellation requested for job #{job_id}"),
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/jobs/{job_id}/pause", include_in_schema=False)
def pause_job(job_id: int, request: Request, db: Session = Depends(get_db)) -> RedirectResponse:
    job = crud.get_job(db, job_id)
    if not job:
        return RedirectResponse(
            url=_url_for(request, "ui_jobs", error="Job not found"),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    if job.status != "running":
        return RedirectResponse(
            url=_url_for(request, "ui_jobs", error="Only running jobs can be paused"),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    crud.mark_pause_requested(db, job)
    crud.create_event(db, job_id=job.id, level="info", message="Pause requested")
    return RedirectResponse(
        url=_url_for(request, "ui_jobs", message=f"Pause requested for job #{job_id}"),
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/jobs/{job_id}/resume", include_in_schema=False)
def resume_job(job_id: int, request: Request, db: Session = Depends(get_db)) -> RedirectResponse:
    job = crud.get_job(db, job_id)
    if not job:
        return RedirectResponse(
            url=_url_for(request, "ui_jobs", error="Job not found"),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    if job.status != "paused":
        return RedirectResponse(
            url=_url_for(request, "ui_jobs", error="Only paused jobs can be resumed"),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    crud.resume_paused_job(db, job)
    crud.create_event(db, job_id=job.id, level="info", message=f"Resumed from {job.progress_done or 0}")
    _ensure_job_worker(request.app)
    return RedirectResponse(
        url=_url_for(request, "ui_jobs", message=f"Job #{job_id} resumed"),
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
            url=_url_for(request, "ui_collection", error=err),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    return RedirectResponse(
        url=_url_for(request, "ui_collection", message=f"Queued bulk job #{job.id}"),
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/collection/batch", include_in_schema=False)
async def start_batch(
    request: Request,
    limit: int = Form(100),
    all_companies: str = Form("false"),
    refresh_zefix: str = Form("false"),
    canton: str = Form(""),
    min_zefix_score: str = Form(""),
    min_claude_score: str = Form(""),
    purpose_keywords: str = Form(""),
    tfidf_cluster: str = Form(""),
    review_status: str = Form(""),
) -> RedirectResponse:
    def _int_or_none(v: str) -> int | None:
        try:
            return int(v.strip()) if v.strip() else None
        except ValueError:
            return None

    job, err = _enqueue_job_safe(
        request,
        job_type="batch",
        label=f"Batch enrichment — up to {limit} companies",
        params={
            "limit": limit,
            "only_missing_website": all_companies != "true",
            "refresh_zefix": refresh_zefix == "true",
            "run_google": True,
            "canton": canton.strip().upper() or None,
            "min_zefix_score": _int_or_none(min_zefix_score),
            "min_claude_score": _int_or_none(min_claude_score),
            "purpose_keywords": purpose_keywords.strip() or None,
            "tfidf_cluster": tfidf_cluster.strip() or None,
            "review_status": review_status.strip() or None,
        },
    )
    if err:
        return RedirectResponse(
            url=_url_for(request, "ui_collection", error=err),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    return RedirectResponse(
        url=_url_for(request, "ui_collection", message=f"Queued batch job #{job.id}"),
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
            url=_url_for(request, "ui_collection", error="Enter at least one company name or UID"),
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
            url=_url_for(request, "ui_collection", error=err),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    return RedirectResponse(
        url=_url_for(request, "ui_collection", message=f"Queued initial search job #{job.id}"),
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
            url=_url_for(request, "ui_collection", error=err),
            status_code=status.HTTP_303_SEE_OTHER,
        )
    return RedirectResponse(
        url=_url_for(request, "ui_collection", message=f"Queued detail job #{job.id}"),
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.post("/ui/collection/dismiss", include_in_schema=False)
def dismiss_task(request: Request) -> RedirectResponse:
    """Clear a finished task so the next run can start."""
    task = getattr(request.app.state, "collection_task", None)
    if task and task.get("done"):
        request.app.state.collection_task = None
    return RedirectResponse(url="/ui/collection", status_code=status.HTTP_303_SEE_OTHER)
