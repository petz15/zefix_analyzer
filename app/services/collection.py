import json
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app import crud
from app.api.google_search_client import search_website
from app.config import settings
from app.api.zefix_client import (
    SWISS_CANTONS,
    _normalise_uid,
    fetch_companies_by_canton,
    get_company as zefix_get_company,
    search_companies,
)
from app.models.company import Company
from app.schemas.company import CompanyCreate, CompanyUpdate
from app.services.scoring import score_result


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

    legal_form_raw = raw.get("legalForm", {})
    if isinstance(legal_form_raw, dict):
        legal_form = legal_form_raw.get("de") or legal_form_raw.get("shortName") or None
    else:
        legal_form = str(legal_form_raw) if legal_form_raw else None

    address_parts = raw.get("address", {}) or {}
    address_str: str | None = None
    if isinstance(address_parts, dict):
        parts = [
            address_parts.get("street"),
            address_parts.get("houseNumber"),
            address_parts.get("swissZipCode"),
            address_parts.get("city"),
        ]
        address_str = " ".join(str(p) for p in parts if p) or None

    uid_normalised = _normalise_uid(str(raw.get("uid", fallback_uid)))

    return CompanyCreate(
        uid=uid_normalised,
        name=name,
        legal_form=legal_form,
        status=str(raw.get("status", "")) or None,
        municipality=raw.get("municipality") or None,
        canton=raw.get("canton") or None,
        purpose=raw.get("purpose") or None,
        address=address_str,
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
    import_limit_per_name: int = 10,
    active_only: bool = True,
    run_google: bool = True,
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
            )
        except Exception as exc:  # noqa: BLE001
            stats["errors"].append(f"Search '{name_clean}': {exc}")
            continue

        for result in results[:import_limit_per_name]:
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


def bulk_import_zefix(
    db: Session,
    *,
    cantons: list[str] | None = None,
    active_only: bool = True,
    page_size: int = 200,
    request_delay: float = 0.5,
    resume: bool = False,
    progress_cb: Any = None,
) -> dict[str, Any]:
    """Import all companies from Zefix by iterating through cantons with pagination.

    Stores basic info (uid, name, legal_form, status, municipality, canton) from
    the canton search API — no per-company detail call is made, keeping request
    volume low. Run ``run_batch_collect`` with ``refresh_zefix=True`` later to
    enrich purpose/address for specific companies.

    Args:
        cantons: List of canton codes to scan. Defaults to all 26.
        active_only: Only import companies with an active register entry.
        page_size: Results per API page (Zefix cap is ~500).
        request_delay: Seconds to sleep between API calls.
        resume: If True, look up the last incomplete run and continue from its checkpoint.
        progress_cb: Optional callable(canton, offset, created, skipped) for logging.
    """
    target_cantons = cantons or SWISS_CANTONS

    # ── Checkpoint / resume ──────────────────────────────────────────────────
    run = None
    start_canton_idx = 0
    start_offset = 0

    if resume:
        run = crud.get_last_incomplete_bulk(db)
        if run and run.last_canton and run.last_canton in target_cantons:
            start_canton_idx = target_cantons.index(run.last_canton)
            start_offset = (run.last_offset or 0) + page_size
            existing_stats: dict[str, Any] = json.loads(run.stats_json or "{}")
        else:
            run = None  # stale checkpoint, start fresh

    if run is None:
        run = crud.create_run(db, "bulk")
        existing_stats = {}

    stats: dict[str, Any] = {
        "cantons_done": existing_stats.get("cantons_done", 0),
        "created": existing_stats.get("created", 0),
        "skipped": existing_stats.get("skipped", 0),
        "errors": existing_stats.get("errors", []),
    }

    # ── Main loop ────────────────────────────────────────────────────────────
    for canton in target_cantons[start_canton_idx:]:
        offset = start_offset if canton == target_cantons[start_canton_idx] else 0

        while True:
            try:
                results = fetch_companies_by_canton(
                    canton,
                    page_size=page_size,
                    offset=offset,
                    active_only=active_only,
                )
            except Exception as exc:  # noqa: BLE001
                stats["errors"].append(f"Canton {canton} offset {offset}: {exc}")
                break

            for result in results:
                if not result.uid:
                    continue
                if crud.get_company_by_uid(db, result.uid):
                    stats["skipped"] += 1
                else:
                    crud.create_company(
                        db,
                        CompanyCreate(
                            uid=result.uid,
                            name=result.name,
                            legal_form=result.legal_form,
                            status=result.status,
                            municipality=result.municipality,
                            canton=result.canton,
                        ),
                    )
                    stats["created"] += 1

            # Persist checkpoint after every page
            crud.update_checkpoint(db, run, canton, offset, stats)

            if progress_cb:
                progress_cb(canton, offset, stats["created"], stats["skipped"])

            if len(results) < page_size:
                break

            offset += page_size
            time.sleep(request_delay)

        stats["cantons_done"] += 1
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

    for company in companies:
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

    return stats
