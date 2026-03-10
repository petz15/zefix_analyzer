import json
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app import crud
from app.api.google_search_client import search_website
from app.config import settings
from app.api.zefix_client import (
    ALPHABET,
    ALPHANUMERIC,
    SWISS_CANTONS,
    ZEFIX_MAX_ENTRIES,
    _normalise_uid,
    _parse_legal_form,
    fetch_companies_by_prefix,
    get_company as zefix_get_company,
    search_companies,
)
from app.models.company import Company
from app.schemas.company import CompanyCreate, CompanyUpdate
from app.services.scoring import score_result

_INDUSTRY_KEYWORDS: list[tuple[str, list[str]]] = [
    ("Technology", ["software", "informatik", "it-", " it ", "digital", "technologie", "saas", "cloud", "künstliche intelligenz", " ai ", " ki ", "automation", "programmier", "developer", "entwicklung von"]),
    ("Construction & Real Estate", ["bau", "immobilien", "real estate", "architektur", "renovati", "gebäude", "liegenschaften", "construction", "haustechnik", "sanitär", "elektroinstallation"]),
    ("Finance", ["finanz", "finance", "invest", "kapital", "versicherung", "insurance", "treuhand", "buchhaltung", "accounting", "steuer", "tax ", "bank", "kredit", "fond", "vermögensverwaltu", "wirtschaftsprüf"]),
    ("Healthcare", ["gesundheit", "health", "medizin", "medical", "pharma", "dental", "therapie", "pflege", "klinik", "arzt", "spital", "praxis"]),
    ("Consulting", ["beratung", "consulting", "management", "strategie", "advisory", "unternehmensberatung"]),
    ("Trade & Retail", ["handel", " trade", "import", "export", "vertrieb", "retail", "grosshandel", "detailhandel", "e-commerce"]),
    ("Hospitality & Food", ["restaurant", "hotel", "gastro", "gastronomie", "catering", "food", "getränke", "beverage", "café", "bäckerei"]),
    ("Manufacturing", ["herstellung", "produktion", "manufacturing", "fertigung", "verarbeit"]),
    ("Transport & Logistics", ["transport", "logistik", "logistics", "spedition", "lieferung", "delivery", "kurier"]),
    ("Education", ["bildung", "education", "schule", "ausbildung", "training", "unterricht", "weiterbildung", "coaching"]),
    ("Marketing & Media", ["marketing", "werbung", "media", "kommunikation", " pr ", "design", "grafik", "agentur", "fotografie", "video"]),
    ("Legal", ["rechts", "legal", "anwalt", "notariat", "kanzlei"]),
    ("Engineering", ["engineering", "ingenieur", "planung", "maschinenbau", "electrical", "civil"]),
]


def _derive_industry(purpose: str | None) -> str | None:
    if not purpose:
        return None
    p = purpose.lower()
    for industry, keywords in _INDUSTRY_KEYWORDS:
        if any(k in p for k in keywords):
            return industry
    return None


def _extract_company_fields(raw: dict[str, Any], fallback_uid: str) -> CompanyCreate:
    name_raw = raw.get("name", "")
    if isinstance(name_raw, dict):
        name = (
            name_raw.get("de")
            or name_raw.get("fr")
            or name_raw.get("it")
            or next(iter(name_raw.values()), "")
        )
    else:
        name = str(name_raw)

    legal_form_display, legal_form_id, legal_form_uid, legal_form_short = _parse_legal_form(
        raw.get("legalForm")
    )

    address_parts = raw.get("address", {}) or {}
    address_str: str | None = None
    if isinstance(address_parts, dict):
        a_parts: list[str] = []
        org = address_parts.get("organisation") or ""
        care_of = address_parts.get("careOf") or ""
        street = address_parts.get("street") or ""
        house_num = address_parts.get("houseNumber") or ""
        addon = address_parts.get("addon") or ""
        po_box = address_parts.get("poBox") or ""
        city = address_parts.get("city") or ""
        zip_code = address_parts.get("swissZipCode") or ""
        if org:
            a_parts.append(org)
        if care_of:
            a_parts.append(f"c/o {care_of}")
        street_line = f"{street} {house_num}".strip()
        if street_line:
            a_parts.append(street_line)
        if addon:
            a_parts.append(addon)
        if po_box:
            a_parts.append(f"Postfach {po_box}")
        zip_city = f"{zip_code} {city}".strip()
        if zip_city:
            a_parts.append(zip_city)
        address_str = ", ".join(a_parts) or None

    uid_normalised = _normalise_uid(str(raw.get("uid", fallback_uid)))

    purpose = raw.get("purpose") or None

    ehraid_raw = raw.get("ehraId") or raw.get("ehraid") or raw.get("ehra_id")
    ehraid = str(ehraid_raw) if ehraid_raw is not None else None

    chid_raw = raw.get("chid")
    chid = str(chid_raw) if chid_raw is not None else None

    legal_seat_id_raw = raw.get("legalSeatId") or raw.get("legal_seat_id")
    legal_seat_id: int | None = None
    if legal_seat_id_raw is not None:
        try:
            legal_seat_id = int(legal_seat_id_raw)
        except (ValueError, TypeError):
            pass

    sogc_date_raw = raw.get("sogcDate") or raw.get("sogc_date")
    sogc_date = str(sogc_date_raw) if sogc_date_raw else None

    deletion_date_raw = raw.get("deletionDate") or raw.get("deletion_date")
    deletion_date = str(deletion_date_raw) if deletion_date_raw else None

    # Extended detail fields
    def _json_field(val: Any) -> str | None:
        return json.dumps(val) if val is not None else None

    sogc_pub_raw = raw.get("sogcPub")
    sogc_pub = _json_field(sogc_pub_raw)

    capital_nominal_raw = raw.get("capitalNominal")
    capital_nominal = str(capital_nominal_raw) if capital_nominal_raw is not None else None
    capital_currency = raw.get("capitalCurrency") or None

    head_offices = _json_field(raw.get("headOffices"))
    further_head_offices = _json_field(raw.get("furtherHeadOffices"))
    branch_offices = _json_field(raw.get("branchOffices"))
    has_taken_over = _json_field(raw.get("hasTakenOver"))
    was_taken_over_by = _json_field(raw.get("wasTakenOverBy"))
    audit_companies = _json_field(raw.get("auditCompanies"))
    old_names = _json_field(raw.get("oldNames"))
    cantonal_excerpt_web = raw.get("cantonalExcerptWeb") or None

    return CompanyCreate(
        uid=uid_normalised,
        name=name,
        legal_form=legal_form_display,
        legal_form_id=legal_form_id,
        legal_form_uid=legal_form_uid,
        legal_form_short_name=legal_form_short,
        status=str(raw.get("status", "")) or None,
        municipality=raw.get("municipality") or raw.get("legalSeat") or None,
        canton=raw.get("canton") or None,
        purpose=purpose,
        address=address_str,
        industry=_derive_industry(purpose),
        ehraid=ehraid,
        chid=chid,
        legal_seat_id=legal_seat_id,
        sogc_date=sogc_date,
        deletion_date=deletion_date,
        sogc_pub=sogc_pub,
        capital_nominal=capital_nominal,
        capital_currency=capital_currency,
        head_offices=head_offices,
        further_head_offices=further_head_offices,
        branch_offices=branch_offices,
        has_taken_over=has_taken_over,
        was_taken_over_by=was_taken_over_by,
        audit_companies=audit_companies,
        old_names=old_names,
        cantonal_excerpt_web=cantonal_excerpt_web,
        zefix_raw=json.dumps(raw),
    )


def import_company_from_zefix_uid(db: Session, uid: str) -> tuple[Company, bool]:
    """Import or update a company from Zefix by UID.

    Returns:
        (company, created)
    """
    raw = zefix_get_company(uid)
    company_data = _extract_company_fields(raw, uid)

    existing = crud.get_company_by_uid(db, company_data.uid)
    if existing:
        # Keep website enrichment data when refreshing core company data from Zefix.
        payload = company_data.model_dump(exclude={"uid", "website_url"})
        updated = crud.update_company(db, existing, CompanyUpdate(**payload))
        return updated, False

    created = crud.create_company(db, company_data)
    return created, True


def enrich_company_website(db: Session, company: Company, *, num: int = 5) -> tuple[bool, str | None]:
    """Fetch top-N Google results, score each against the company profile, and persist.

    Stores all scored results in google_search_results_raw (JSON).
    Sets website_url and website_match_score to the best-scoring result.
    Always sets website_checked_at so callers know a search was attempted.
    """
    now = datetime.now(tz=timezone.utc)
    results = search_website(company.name, num=num)

    if not results:
        crud.update_company(
            db,
            company,
            CompanyUpdate(
                website_checked_at=now,
                google_search_results_raw=json.dumps([]),
            ),
        )
        return False, None

    scored: list[dict] = []
    for r in results:
        s = score_result(
            {"title": r.title, "link": r.link, "snippet": r.snippet or ""},
            company_name=company.name,
            municipality=company.municipality,
            canton=company.canton,
            purpose=company.purpose,
            legal_form=company.legal_form,
        )
        scored.append({"title": r.title, "link": r.link, "snippet": r.snippet or "", "score": s})

    scored.sort(key=lambda x: x["score"], reverse=True)
    best = scored[0]

    crud.update_company(
        db,
        company,
        CompanyUpdate(
            website_url=best["link"],
            website_match_score=best["score"],
            website_checked_at=now,
            google_search_results_raw=json.dumps(scored),
        ),
    )
    return True, best["link"]


def initial_collect(
    db: Session,
    *,
    names: list[str],
    uids: list[str],
    search_max_results: int = 25,
    active_only: bool = True,
    run_google: bool = True,
    canton: str | None = None,
    legal_form: str | None = None,
) -> dict[str, Any]:
    """Run a one-time collection from explicit UIDs and search terms."""
    stats: dict[str, Any] = {
        "created": 0,
        "updated": 0,
        "google_enriched": 0,
        "google_no_result": 0,
        "errors": [],
    }

    for uid in uids:
        uid_clean = uid.strip()
        if not uid_clean:
            continue
        try:
            company, created = import_company_from_zefix_uid(db, uid_clean)
            stats["created" if created else "updated"] += 1
            if run_google:
                enriched, _ = enrich_company_website(db, company)
                if enriched:
                    stats["google_enriched"] += 1
                else:
                    stats["google_no_result"] += 1
        except Exception as exc:  # noqa: BLE001
            stats["errors"].append(f"UID {uid_clean}: {exc}")

    for name in names:
        name_clean = name.strip()
        if not name_clean:
            continue
        try:
            results = search_companies(
                name_clean,
                max_results=search_max_results,
                active_only=active_only,
                canton=canton,
                legal_form=legal_form,
            )
        except Exception as exc:  # noqa: BLE001
            stats["errors"].append(f"Search '{name_clean}': {exc}")
            continue

        for result in results:
            try:
                company, created = import_company_from_zefix_uid(db, result.uid)
                stats["created" if created else "updated"] += 1
                if run_google:
                    enriched, _ = enrich_company_website(db, company)
                    if enriched:
                        stats["google_enriched"] += 1
                    else:
                        stats["google_no_result"] += 1
            except Exception as exc:  # noqa: BLE001
                stats["errors"].append(f"UID {result.uid} from search '{name_clean}': {exc}")

    return stats


def _fetch_prefix_with_fallback(
    canton: str | None,
    prefix: str,
    active_only: bool,
    request_delay: float,
    _depth: int = 0,
) -> list[Any]:
    """Return all companies for *prefix*, expanding to longer prefixes when the cap is hit.

    Expands up to three levels deep (single → double → triple letter/digit prefix).
    Expansion is triggered when:
    - The query returns exactly ZEFIX_MAX_ENTRIES results (API truncated), or
    - The query returns HTTP 400 (Zefix rejects oversized result sets).

    Results across all sub-prefixes are deduplicated by UID.
    """
    _MAX_DEPTH = 2  # 0 = single, 1 = double, 2 = triple

    expand = False
    results: list[Any] = []
    try:
        results = fetch_companies_by_prefix(prefix, canton, active_only=active_only)
        if len(results) >= ZEFIX_MAX_ENTRIES:
            expand = True
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 400:
            expand = True
        else:
            raise

    if not expand:
        return results

    if _depth >= _MAX_DEPTH:
        # Cannot expand further — return what we have (may be partial)
        return results

    # Expand to next-level sub-prefixes using full alphanumeric set
    seen: set[str] = set()
    expanded: list[Any] = []

    for char in ALPHANUMERIC:
        sub_prefix = prefix + char
        try:
            sub_results = _fetch_prefix_with_fallback(
                canton, sub_prefix, active_only, request_delay, _depth + 1
            )
        except Exception:  # noqa: BLE001
            continue
        for r in sub_results:
            if r.uid and r.uid not in seen:
                seen.add(r.uid)
                expanded.append(r)
        time.sleep(request_delay)

    return expanded


def bulk_import_zefix(
    db: Session,
    *,
    cantons: list[str] | None = None,
    active_only: bool = True,
    request_delay: float = 0.5,
    resume: bool = False,
    progress_cb: Any = None,
) -> dict[str, Any]:
    """Import all companies from Zefix using an alphabet-prefix sweep per canton.

    For each canton, queries A–Z as name prefixes.  If any single-letter query
    returns the API's hard cap (500), it automatically expands that letter into
    26 two-letter sub-prefixes (e.g. "Ba" … "Bz") to capture the full set.

    Checkpoint stored as (canton, prefix_index) so the run can be resumed after
    an interruption.  ``last_offset`` stores the 0-based index into ALPHABET
    (0 = "A", 25 = "Z") of the last completed prefix for the current canton.

    Args:
        cantons: Canton codes to scan. Defaults to all 26.
        active_only: Only import active register entries.
        request_delay: Seconds to sleep between API calls.
        resume: Continue from the last saved checkpoint if one exists.
        progress_cb: Optional callable(canton, prefix, created, skipped).
    """
    target_cantons = cantons or SWISS_CANTONS

    # ── Checkpoint / resume ──────────────────────────────────────────────────
    run = None
    start_canton_idx = 0
    start_prefix_idx = 0

    if resume:
        run = crud.get_last_incomplete_bulk(db)
        if run and run.last_canton and run.last_canton in target_cantons:
            start_canton_idx = target_cantons.index(run.last_canton)
            # resume after the last completed prefix
            start_prefix_idx = (run.last_offset or 0) + 1
            existing_stats: dict[str, Any] = json.loads(run.stats_json or "{}")
        else:
            run = None  # stale / unrelated checkpoint

    if run is None:
        run = crud.create_run(db, "bulk")
        existing_stats = {}

    stats: dict[str, Any] = {
        "cantons_done": existing_stats.get("cantons_done", 0),
        "created": existing_stats.get("created", 0),
        "updated": existing_stats.get("updated", 0),
        "errors": existing_stats.get("errors", []),
    }

    # Fields that come exclusively from Zefix — safe to overwrite on every run
    _ZEFIX_UPDATE_FIELDS = {
        "name", "legal_form", "legal_form_id", "legal_form_uid", "legal_form_short_name",
        "status", "municipality", "canton", "purpose", "industry",
        "ehraid", "chid", "legal_seat_id", "sogc_date", "deletion_date",
    }

    # ── Main sweep ───────────────────────────────────────────────────────────
    for canton in target_cantons[start_canton_idx:]:
        prefix_start = start_prefix_idx if canton == target_cantons[start_canton_idx] else 0

        for prefix_idx, char in enumerate(ALPHANUMERIC):
            if prefix_idx < prefix_start:
                continue

            try:
                results = _fetch_prefix_with_fallback(
                    canton, char, active_only=active_only, request_delay=request_delay
                )
            except Exception as exc:  # noqa: BLE001
                stats["errors"].append(f"Canton {canton} prefix {char}: {exc}")
                crud.update_checkpoint(db, run, canton, prefix_idx, stats)
                time.sleep(request_delay)
                continue

            for result in results:
                if not result.uid:
                    continue
                company_data = CompanyCreate(
                    uid=result.uid,
                    name=result.name,
                    legal_form=result.legal_form,
                    legal_form_id=result.legal_form_id,
                    legal_form_uid=result.legal_form_uid,
                    legal_form_short_name=result.legal_form_short_name,
                    status=result.status,
                    municipality=result.municipality,
                    canton=result.canton or canton,  # search was canton-scoped, so always known
                    purpose=result.purpose,
                    industry=_derive_industry(result.purpose),
                    ehraid=result.ehraid,
                    chid=result.chid,
                    legal_seat_id=result.legal_seat_id,
                    sogc_date=result.sogc_date,
                    deletion_date=result.deletion_date,
                )
                existing = crud.get_company_by_uid(db, result.uid)
                if existing:
                    update_payload = {
                        k: v for k, v in company_data.model_dump(exclude={"uid"}).items()
                        if k in _ZEFIX_UPDATE_FIELDS
                    }
                    crud.update_company(db, existing, CompanyUpdate(**update_payload))
                    stats["updated"] += 1
                else:
                    crud.create_company(db, company_data)
                    stats["created"] += 1

            # Checkpoint: last_offset = prefix index (0–35 for 0-9 + A-Z)
            crud.update_checkpoint(db, run, canton, prefix_idx, stats)

            if progress_cb:
                progress_cb(canton, char, stats["created"], stats["updated"])

            time.sleep(request_delay)

        stats["cantons_done"] += 1
        start_prefix_idx = 0  # reset for subsequent cantons
        time.sleep(request_delay)

    crud.complete_run(db, run, stats)
    return stats


def rescore_from_stored_results(db: Session, company: Company) -> bool:
    """Re-score website_match_score from already-stored google_search_results_raw.

    Called after a Zefix detail refresh so that freshly fetched purpose /
    municipality / canton data is applied to the existing Google results without
    spending a new API call.  Only updates score fields; never triggers a new
    Google search.

    Returns True if scoring was applied and saved, False otherwise.
    """
    if not company.google_search_results_raw:
        return False
    try:
        stored: list[dict] = json.loads(company.google_search_results_raw)
    except (json.JSONDecodeError, TypeError):
        return False
    if not stored:
        return False

    rescored = []
    for r in stored:
        s = score_result(
            {"title": r.get("title", ""), "link": r.get("link", ""), "snippet": r.get("snippet", "")},
            company_name=company.name,
            municipality=company.municipality,
            canton=company.canton,
            purpose=company.purpose,
            legal_form=company.legal_form,
        )
        rescored.append({**r, "score": s})

    rescored.sort(key=lambda x: x["score"], reverse=True)
    best = rescored[0]
    crud.update_company(
        db,
        company,
        CompanyUpdate(
            website_url=best["link"],
            website_match_score=best["score"],
            google_search_results_raw=json.dumps(rescored),
        ),
    )
    return True


def run_zefix_detail_collect(
    db: Session,
    *,
    cantons: list[str] | None = None,
    uids: list[str] | None = None,
    limit: int = 500,
    skip: int = 0,
    score_if_missing: bool = True,
    request_delay: float = 0.3,
    progress_cb: Any = None,
) -> dict[str, Any]:
    """Fetch full Zefix detail records for companies already in the database.

    Calls the per-UID Zefix endpoint for each target company, updating address,
    purpose, legal form, status, and zefix_raw JSON.  Optionally re-scores
    existing Google results with the freshly fetched purpose / municipality data.

    Targeting priority:
        1. Explicit *uids* list.
        2. *cantons* filter (all DB companies in those cantons).
        3. All companies, paginated by *limit* / *skip*.

    Scoring applies only when *score_if_missing* is True **and** the company has
    stored Google results but no ``website_match_score`` yet.
    """
    stats: dict[str, Any] = {
        "selected": 0,
        "updated": 0,
        "scored": 0,
        "errors": [],
    }

    run = crud.create_run(db, "detail")

    # ── Build target list ─────────────────────────────────────────────────────
    if uids:
        companies: list[Company] = [
            c for uid in uids if (c := crud.get_company_by_uid(db, uid)) is not None
        ]
    elif cantons:
        companies = (
            db.query(Company)
            .filter(Company.canton.in_(cantons))
            .offset(skip)
            .limit(limit)
            .all()
        )
    else:
        companies = db.query(Company).offset(skip).limit(limit).all()

    stats["selected"] = len(companies)
    total = len(companies)

    for i, company in enumerate(companies, 1):
        try:
            updated, _ = import_company_from_zefix_uid(db, company.uid)
            stats["updated"] += 1

            # Re-score only when no score exists yet and stored results are available
            if score_if_missing and updated.website_match_score is None:
                if rescore_from_stored_results(db, updated):
                    stats["scored"] += 1

        except Exception as exc:  # noqa: BLE001
            stats["errors"].append(f"{company.uid} ({company.name}): {exc}")

        if progress_cb:
            progress_cb(i, total, stats)

        # Periodic checkpoint (every 50 companies)
        if i % 50 == 0:
            crud.update_checkpoint(db, run, company.canton or "—", i, stats)

        time.sleep(request_delay)

    crud.complete_run(db, run, stats)
    return stats


def run_batch_collect(
    db: Session,
    *,
    limit: int = 200,
    skip: int = 0,
    only_missing_website: bool = True,
    refresh_zefix: bool = False,
    run_google: bool = True,
    progress_cb: Any = None,
) -> dict[str, Any]:
    """Run a recurring batch process over companies already in your DB."""
    stats: dict[str, Any] = {
        "selected": 0,
        "zefix_refreshed": 0,
        "google_enriched": 0,
        "google_no_result": 0,
        "errors": [],
    }

    if run_google:
        quota = settings.google_daily_quota
        searches_today = crud.get_company_stats(db)["searches_today"]
        available = max(0, quota - searches_today)
        if available == 0:
            stats["errors"].append(
                f"Daily Google quota of {quota} already reached; skipping Google enrichment."
            )
            run_google = False
        elif limit > available:
            limit = available

    query = db.query(Company).order_by(Company.id.asc())
    if only_missing_website:
        query = query.filter(or_(Company.website_url.is_(None), Company.website_url == ""))

    companies = query.offset(skip).limit(limit).all()
    stats["selected"] = len(companies)

    for i, company in enumerate(companies, 1):
        current = company
        if refresh_zefix:
            try:
                refreshed, _ = import_company_from_zefix_uid(db, company.uid)
                current = refreshed
                stats["zefix_refreshed"] += 1
            except Exception as exc:  # noqa: BLE001
                stats["errors"].append(f"Zefix refresh {company.uid}: {exc}")

        if run_google:
            try:
                enriched, _ = enrich_company_website(db, current)
                if enriched:
                    stats["google_enriched"] += 1
                else:
                    stats["google_no_result"] += 1
            except Exception as exc:  # noqa: BLE001
                stats["errors"].append(f"Google search {current.uid}: {exc}")

        if progress_cb:
            progress_cb(i, stats["selected"], stats)

    return stats
