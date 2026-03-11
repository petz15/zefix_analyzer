import json
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app import crud
from app.api.geocoding_client import geocode_address
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
from app.services.scoring import (
    compute_zefix_score,
    compute_zefix_score_breakdown,
    distance_to_muri_km,
    fallback_result_score,
    get_default_scoring_config,
    is_irrelevant_result,
    is_social_lead_domain,
    score_result,
)

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


# Stopwords for TF-IDF vectorization (German + French + Italian + English + Swiss registry boilerplate)
# Goal: remove words that appear in almost every company purpose and don't help distinguish clusters.
# Do NOT add words that mark meaningful categories (e.g. "handel", "entwicklung", "bau", "immobilien").
_TFIDF_STOPWORDS: set[str] = {
    # ── German: articles, pronouns, conjunctions, prepositions ───────────────
    "die", "der", "das", "und", "oder", "mit", "von", "für", "des", "dem",
    "den", "ein", "eine", "einer", "eines", "sich", "auf", "zu", "ist",
    "sowie", "als", "auch", "nicht", "nach", "bei", "alle", "durch", "wird",
    "deren", "diese", "dieser", "dieses", "sie", "ihr", "ihren", "ihres",
    "haben", "hat", "hatte", "werden", "war", "sind", "sein",
    "im", "an", "am", "ab", "um", "bis", "vor", "aus", "über", "unter",
    "zum", "zur", "beim", "vom", "ans", "ins", "er", "es", "wir",
    "ihm", "ihn", "ihnen", "uns", "man", "kein", "keine",
    "diesem", "diesen", "solche", "solcher", "welche", "welcher",
    "jede", "jeder", "jedes", "aller", "allem", "allen",
    "jedoch", "daher", "dabei", "dazu", "davon", "darüber", "dafür",
    "soweit", "sowohl", "darunter", "hierzu", "hierbei", "hierfür", "bzgl", "bzw",
    # ── Swiss registry boilerplate (appear in nearly every purpose text) ──────
    "gesellschaft", "gesellschaften", "gesellschafts", "unternehmen", "betrieb", "zweck", "zwecks",
    "aktien", "gmbh", "ag", "sarl", "sàrl", "cie", "co", "inc",
    "insbesondere", "namentlich", "hauptsächlich", "vorzugsweise",
    "allgemein", "allgemeine", "allgemeinen", "sonstige", "sonstigen",
    "ähnliche", "ähnlichen", "weitere", "weiteren", "entsprechende",
    "verschiedene", "verschiedenen", "verschiedenste",
    "konzern", "konzerne", "gruppe", "gruppen",
    "hauptsitz", "sitz", "domizil", "domizile",
    "schweiz", "schweizer", "schweizerische", "schweizerischen",
    "europa", "europäische", "europäischen",
    "weltweit", "international", "global", "national",
    # Generic activity words — too broad to form meaningful clusters
    "erbringung", "dienstleistungen", "dienstleistung", "leistungen", "leistung",
    "tätigkeiten", "tätigkeit", "aktivitäten", "aktivität",
    "verwaltung", "führung", "betreuung",
    "bereich", "bereiche", "bereichen", "gebiet", "gebiete", "gebieten",
    "erwerb", "erwerben", "veräusserung", "veräussern",
    "beteiligung", "beteiligungen", "beteiligen", "halten", "verwalten", "betreiben",
    "erbringen", "anbieten", "durchführen", "ausführen",
    # ── Swiss registry standard boilerplate sentence (verbatim filler) ────────
    '''Kann Zweigniederlassungen und Tochtergesellschaften im In- und Ausland
      errichten, sich an anderen Unternehmen beteiligen, alle Geschäfte tätigen,
     die direkt oder indirekt mit ihrem Zweck in Zusammenhang stehen, im In-
      und Ausland Grundeigentum erwerben, belasten, veräussern und verwalten,
      Finanzierungen vornehmen sowie Garantien und Bürgschaften eingehen.''',
    "kann", "errichten", "anderen", "geschäfte", "geschäftstätigkeit", "geschäftstätigkeiten",
    "tätigen", "direkt", "indirekt", "ihrem", "zusammenhang", "stehen",
    "grundeigentum", "belasten", "finanzierungen", "eigene", "fremde", "rechnung",
    "vornehmen", "garantien", "bürgschaften", "dritte", "eingehen",
    "tochtergesellschaft", "tochtergesellschaften",
    "zweigniederlassung", "zweigniederlassungen", "niederlassung", "niederlassungen",
    "inland", "ausland", "verbundenen",
    # ── French: articles, prepositions, pronouns ─────────────────────────────
    "les", "une", "est", "dans", "par", "sur", "aux",
    "de", "la", "le", "et", "en", "du", "au", "avec", "qui", "que",
    "se", "son", "sa", "ses", "toute", "tous", "toutes",
    "il", "ils", "elle", "elles", "nous", "vous", "leur", "leurs",
    "ce", "cet", "cette", "ces", "ou", "mais", "donc",
    "pour", "pas", "plus", "comme", "aussi",
    "notamment", "ainsi", "dont", "afin", "selon",
    # ── Italian: articles, prepositions, pronouns ────────────────────────────
    "di", "il", "e", "del", "della", "dello", "dei", "delle",
    "un", "una", "su", "con", "per", "al", "alla", "alle", "ai",
    "che", "sono", "ed", "ha", "hanno", "si", "da", "dal",
    "dalla", "dai", "dagli", "tra", "fra", "lo", "gli",
    "ne", "ci", "non", "anche", "come", "tutti", "ogni",
    # ── English ───────────────────────────────────────────────────────────────
    "the", "and", "of", "in", "for", "to", "a", "an", "with", "its",
    "as", "by", "at", "from", "or", "be", "is", "are", "was", "were",
    "have", "has", "had", "will", "can", "all", "any",
    "other", "such", "their", "this", "that", "these", "those",
    "including", "related", "services", "company", "activities",
    "general", "various", "especially", "particular",
}

# Default system prompt for Claude classification
_DEFAULT_CLAUDE_PROMPT = (
    "You are evaluating Swiss company register (Zefix) entries as potential B2B leads. "
    "Given the company name, purpose statement, and optionally an industry label, output ONLY a JSON object "
    "(no markdown, no explanation) with exactly two fields:\n"
    '- "score": integer 0-100 (100 = perfect lead, 0 = completely irrelevant)\n'
    '- "category": short English category label (e.g. "Technology", "Manufacturing", "Consulting", "Retail")\n\n'
    "Consider: Is the company an active SME likely to need services or products? "
    "Holding companies, pure financial entities, and non-commercial associations score lower."
)


def _derive_industry(
    purpose: str | None,
    taxonomy: list[tuple[str, list[str]]] | None = None,
) -> str | None:
    """Return the best-matching industry category using keyword hit count (best-match, not first-match)."""
    if not purpose:
        return None
    p = purpose.lower()
    entries = taxonomy if taxonomy is not None else _INDUSTRY_KEYWORDS
    best_cat: str | None = None
    best_hits = 0
    for industry, keywords in entries:
        hits = sum(1 for k in keywords if k in p)
        if hits > best_hits:
            best_cat = industry
            best_hits = hits
    return best_cat


def _load_industry_taxonomy(db: Session) -> list[tuple[str, list[str]]]:
    """Load industry keyword taxonomy from settings (falls back to hardcoded defaults).

    DB format — one category per line:
        Technology: software, informatik, it-, digital
        Finance: finanz, buchhaltung, steuer
    """
    raw = crud.get_setting(db, "industry_taxonomy", "").strip()
    if not raw:
        return _INDUSTRY_KEYWORDS
    result: list[tuple[str, list[str]]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        cat, kw_str = line.split(":", 1)
        cat = cat.strip()
        keywords = [k.strip().lower() for k in kw_str.split(",") if k.strip()]
        if cat and keywords:
            result.append((cat, keywords))
    return result if result else _INDUSTRY_KEYWORDS


def _load_scoring_config(db: Session) -> dict[str, str]:
    defaults = get_default_scoring_config()
    return {key: crud.get_setting(db, key, val) for key, val in defaults.items()}


def _extract_company_fields(
    raw: dict[str, Any],
    fallback_uid: str,
    *,
    scoring_config: dict[str, str] | None = None,
    taxonomy: list[tuple[str, list[str]]] | None = None,
) -> CompanyCreate:
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

    industry = _derive_industry(purpose, taxonomy)
    score_breakdown = compute_zefix_score_breakdown(
        legal_form=legal_form_display,
        legal_form_short_name=legal_form_short,
        capital_nominal=capital_nominal,
        purpose=purpose,
        branch_offices=branch_offices,
        industry=industry,
        status=str(raw.get("status", "")) or None,
        canton=raw.get("canton") or None,
        municipality=raw.get("municipality") or raw.get("legalSeat") or None,
        config=scoring_config,
    )
    zefix_score = int(score_breakdown["final_score"])

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
        industry=industry,
        zefix_score=zefix_score,
        zefix_score_breakdown=json.dumps(score_breakdown),
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
    scoring_config = _load_scoring_config(db)
    taxonomy = _load_industry_taxonomy(db)
    company_data = _extract_company_fields(raw, uid, scoring_config=scoring_config, taxonomy=taxonomy)

    existing = crud.get_company_by_uid(db, company_data.uid)
    if existing:
        # Keep website enrichment data when refreshing core company data from Zefix.
        payload = company_data.model_dump(exclude={"uid", "website_url"})
        updated = crud.update_company(db, existing, CompanyUpdate(**payload))
        return updated, False

    created = crud.create_company(db, company_data)
    return created, True


def geocode_and_update_company(db: Session, company: Company) -> bool:
    """Geocode the company's address and persist lat/lon + recompute zefix_score.

    Skipped when the company has no address or coordinates are already set.

    Returns:
        True if coordinates were successfully obtained and saved.
    """
    if not company.address:
        return False
    if company.lat is not None and company.lon is not None:
        return False

    coords = geocode_address(company.address)
    if coords is None:
        return False

    lat, lon = coords
    scoring_config = _load_scoring_config(db)
    score_breakdown = compute_zefix_score_breakdown(
        legal_form=company.legal_form,
        legal_form_short_name=company.legal_form_short_name,
        capital_nominal=company.capital_nominal,
        purpose=company.purpose,
        branch_offices=company.branch_offices,
        industry=company.industry,
        status=company.status,
        canton=company.canton,
        municipality=company.municipality,
        lat=lat,
        lon=lon,
        config=scoring_config,
    )
    crud.update_company(
        db,
        company,
        CompanyUpdate(
            lat=lat,
            lon=lon,
            zefix_score=int(score_breakdown["final_score"]),
            zefix_score_breakdown=json.dumps(score_breakdown),
        ),
    )
    return True


def recalculate_zefix_scores(
    db: Session,
    *,
    batch_size: int = 500,
    resume_from: int = 0,
    progress_cb: Any = None,
) -> dict[str, Any]:
    """Recompute zefix_score for every company using the current scoring algorithm.

    Useful after changing scoring weights — no network calls are made.
    Commits in batches of *batch_size* to avoid holding a huge transaction.

    Returns:
        ``{"updated": int, "errors": list[str]}``
    """
    stats: dict[str, Any] = {"updated": 0, "geocoded": 0, "errors": []}
    scoring_config = _load_scoring_config(db)

    total = db.query(Company).count()
    offset = max(0, min(resume_from, total))

    while True:
        batch = db.query(Company).order_by(Company.id.asc()).offset(offset).limit(batch_size).all()
        if not batch:
            break

        for company in batch:
            try:
                # Geocode if lat/lon not yet set
                if company.lat is None and company.lon is None and company.address:
                    coords = geocode_address(company.address)
                    if coords:
                        company.lat, company.lon = coords
                        stats["geocoded"] += 1

                score_breakdown = compute_zefix_score_breakdown(
                    legal_form=company.legal_form,
                    legal_form_short_name=company.legal_form_short_name,
                    capital_nominal=company.capital_nominal,
                    purpose=company.purpose,
                    branch_offices=company.branch_offices,
                    industry=company.industry,
                    status=company.status,
                    canton=company.canton,
                    municipality=company.municipality,
                    lat=company.lat,
                    lon=company.lon,
                    config=scoring_config,
                )
                company.zefix_score = int(score_breakdown["final_score"])
                company.zefix_score_breakdown = json.dumps(score_breakdown)
                stats["updated"] += 1
            except Exception as exc:  # noqa: BLE001
                stats["errors"].append(f"{company.uid}: {exc}")

        db.commit()
        offset += len(batch)

        if progress_cb:
            progress_cb(min(offset, total), total, stats)

    return stats


def re_geocode_all_companies(
    db: Session,
    *,
    batch_size: int = 500,
    resume_from: int = 0,
    progress_cb: Any = None,
) -> dict[str, Any]:
    """Re-geocode every company that has an address, overwriting existing lat/lon.

    Used for the one-time upgrade from PLZ-centroid to building-level coordinates
    after the swisstopo dataset becomes available.  No network calls beyond the
    initial geocoding DB download (already done at Docker build time).

    Returns:
        ``{"geocoded": int, "failed": int, "skipped": int, "errors": list[str]}``
    """
    stats: dict[str, Any] = {"geocoded": 0, "failed": 0, "skipped": 0, "errors": []}

    total = db.query(Company).filter(Company.address.isnot(None)).count()
    offset = max(0, min(resume_from, total))

    while True:
        batch = (
            db.query(Company)
            .filter(Company.address.isnot(None))
            .order_by(Company.id.asc())
            .offset(offset)
            .limit(batch_size)
            .all()
        )
        if not batch:
            break

        for company in batch:
            try:
                coords = geocode_address(company.address)
                if coords:
                    company.lat, company.lon = coords
                    stats["geocoded"] += 1
                else:
                    stats["failed"] += 1
            except Exception as exc:  # noqa: BLE001
                stats["errors"].append(f"{company.uid}: {exc}")

        db.commit()
        offset += len(batch)

        if progress_cb:
            progress_cb(min(offset, total), total, stats)

    return stats


def _score_google_results_for_company(company: Company, raw_results: list[dict]) -> list[dict]:
    """Score and sort Google results for one company using current scoring rules."""
    if not raw_results:
        return []

    top_window = raw_results[: min(3, len(raw_results))]
    irrelevant_count = sum(
        1 for rr in top_window
        if is_irrelevant_result(rr, company_name=company.name)
    )
    use_fallback = bool(top_window) and irrelevant_count >= ((len(top_window) + 1) // 2)
    social_in_top = any(is_social_lead_domain((rr.get("link") or "")) for rr in top_window)

    scored: list[dict] = []
    for rr in raw_results:
        row = {
            "title": rr.get("title", "") or "",
            "link": rr.get("link", "") or "",
            "snippet": rr.get("snippet", "") or "",
        }
        if use_fallback:
            s = fallback_result_score(
                row,
                municipality=company.municipality,
                canton=company.canton,
                legal_form=company.legal_form,
            )
        else:
            s = score_result(
                row,
                company_name=company.name,
                municipality=company.municipality,
                canton=company.canton,
                purpose=company.purpose,
                legal_form=company.legal_form,
            )

        if social_in_top and is_social_lead_domain(row["link"]):
            s = min(100, s + 15)

        scored.append({**row, "score": s})

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored


def recalculate_google_scores(
    db: Session,
    *,
    batch_size: int = 500,
    resume_from: int = 0,
    progress_cb: Any = None,
) -> dict[str, Any]:
    """Recompute website_match_score from stored Google results for all companies."""
    stats: dict[str, Any] = {"updated": 0, "skipped": 0, "errors": []}

    total = db.query(Company).count()
    offset = max(0, min(resume_from, total))

    while True:
        batch = db.query(Company).order_by(Company.id.asc()).offset(offset).limit(batch_size).all()
        if not batch:
            break

        for company in batch:
            try:
                if not company.google_search_results_raw:
                    stats["skipped"] += 1
                    continue

                raw_results = json.loads(company.google_search_results_raw)
                if not isinstance(raw_results, list) or not raw_results:
                    stats["skipped"] += 1
                    continue

                rescored = _score_google_results_for_company(company, raw_results)
                if not rescored:
                    stats["skipped"] += 1
                    continue

                best = rescored[0]
                company.website_url = best["link"]
                company.website_match_score = best["score"]
                company.google_search_results_raw = json.dumps(rescored)
                stats["updated"] += 1
            except Exception as exc:  # noqa: BLE001
                stats["errors"].append(f"{company.uid}: {exc}")

        db.commit()
        offset += len(batch)

        if progress_cb:
            progress_cb(min(offset, total), total, stats)

    return stats


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

    raw_results = [{"title": r.title, "link": r.link, "snippet": r.snippet or ""} for r in results]
    scored = _score_google_results_for_company(company, raw_results)
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
    resume_from: int = 0,
    progress_cb: Any = None,
) -> dict[str, Any]:
    """Run a one-time collection from explicit UIDs and search terms."""
    stats: dict[str, Any] = {
        "created": 0,
        "updated": 0,
        "google_enriched": 0,
        "google_no_result": 0,
        "errors": [],
    }

    target_uids: list[str] = []
    seen: set[str] = set()

    for uid in uids:
        uid_clean = uid.strip()
        if not uid_clean or uid_clean in seen:
            continue
        seen.add(uid_clean)
        target_uids.append(uid_clean)

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
            uid_clean = (result.uid or "").strip()
            if not uid_clean or uid_clean in seen:
                continue
            seen.add(uid_clean)
            target_uids.append(uid_clean)

    total = len(target_uids)
    start_idx = max(0, min(resume_from, total))

    for idx, uid_clean in enumerate(target_uids[start_idx:], start=start_idx + 1):
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
        if progress_cb:
            progress_cb(idx, total, stats)

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
    scoring_config = _load_scoring_config(db)
    taxonomy = _load_industry_taxonomy(db)

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
        "status", "municipality", "canton", "purpose", "industry", "zefix_score", "zefix_score_breakdown",
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
                _bulk_industry = _derive_industry(result.purpose, taxonomy)
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
                    industry=_bulk_industry,
                    zefix_score=compute_zefix_score(
                        legal_form=result.legal_form,
                        legal_form_short_name=result.legal_form_short_name,
                        capital_nominal=None,
                        purpose=result.purpose,
                        branch_offices=None,
                        industry=_bulk_industry,
                        status=result.status,
                        canton=result.canton or canton,
                        municipality=result.municipality,
                        config=scoring_config,
                    ),
                    zefix_score_breakdown=json.dumps(
                        compute_zefix_score_breakdown(
                            legal_form=result.legal_form,
                            legal_form_short_name=result.legal_form_short_name,
                            capital_nominal=None,
                            purpose=result.purpose,
                            branch_offices=None,
                            industry=_bulk_industry,
                            status=result.status,
                            canton=result.canton or canton,
                            municipality=result.municipality,
                            config=scoring_config,
                        )
                    ),
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

    rescored = _score_google_results_for_company(company, stored)
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
    score_if_missing: bool = True,
    only_missing_details: bool = False,
    resume_from: int = 0,
    request_delay: float = 0.3,
    progress_cb: Any = None,
) -> dict[str, Any]:
    """Fetch full Zefix detail records for companies already in the database.

    Calls the per-UID Zefix endpoint for each target company, updating address,
    purpose, legal form, status, and zefix_raw JSON.  Optionally re-scores
    existing Google results with the freshly fetched purpose / municipality data.

    Targeting priority:
        1. Explicit *uids* list — processes exactly those companies.
        2. *cantons* filter — all DB companies in those cantons.
        3. No filter — all companies in the database.

    When *only_missing_details* is True, limits to companies that are missing
    at least one of: address, purpose, or cantonal_excerpt_web.

    Scoring applies only when *score_if_missing* is True **and** the company has
    stored Google results but no ``website_match_score`` yet.
    """
    stats: dict[str, Any] = {
        "selected": 0,
        "updated": 0,
        "scored": 0,
        "geocoded": 0,
        "errors": [],
    }

    run = crud.create_run(db, "detail")

    # ── Build target list ─────────────────────────────────────────────────────
    # For query-based targeting, prioritise companies that have never received
    # a full detail fetch (no purpose, no zefix_score, no zefix_raw) so that
    # new/thin records are enriched before re-fetching already-detailed ones.
    def _detail_priority_order(q):
        """Order: un-detailed companies first, then by zefix_score desc."""
        from sqlalchemy import case
        has_detail = case(
            (Company.zefix_raw.isnot(None), 1),
            else_=0,
        )
        return q.order_by(has_detail.asc(), Company.zefix_score.desc().nullslast(), Company.id.asc())

    _missing_details_filter = or_(
        Company.address.is_(None),
        Company.purpose.is_(None),
        Company.cantonal_excerpt_web.is_(None),
    )

    if uids:
        companies: list[Company] = [
            c for uid in uids if (c := crud.get_company_by_uid(db, uid)) is not None
        ]
        if only_missing_details:
            companies = [
                c for c in companies
                if c.address is None or c.purpose is None or c.cantonal_excerpt_web is None
            ]
    elif cantons:
        q = db.query(Company).filter(Company.canton.in_(cantons))
        if only_missing_details:
            q = q.filter(_missing_details_filter)
        companies = _detail_priority_order(q).all()
    else:
        q = db.query(Company)
        if only_missing_details:
            q = q.filter(_missing_details_filter)
        companies = _detail_priority_order(q).all()

    stats["selected"] = len(companies)
    total = len(companies)

    start_idx = max(0, min(resume_from, total))
    for i, company in enumerate(companies[start_idx:], start=start_idx + 1):
        try:
            updated, _ = import_company_from_zefix_uid(db, company.uid)
            stats["updated"] += 1

            # Geocode address → lat/lon (skipped if already set; Nominatim rate-limits itself)
            if geocode_and_update_company(db, updated):
                stats["geocoded"] += 1

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
    only_missing_website: bool = True,
    refresh_zefix: bool = False,
    run_google: bool = True,
    resume_from: int = 0,
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

    query = db.query(Company)
    if only_missing_website:
        query = query.filter(or_(Company.website_url.is_(None), Company.website_url == ""))

    candidates = query.all()

    def _batch_order_key(company: Company) -> tuple[float, float, int]:
        score = float(company.zefix_score) if company.zefix_score is not None else float("-inf")
        distance = distance_to_muri_km(
            canton=company.canton,
            municipality=company.municipality,
            lat=company.lat,
            lon=company.lon,
        )
        return (-score, distance if distance is not None else float("inf"), company.id)

    ordered = sorted(candidates, key=_batch_order_key)
    planned = ordered[:limit]
    start_idx = max(0, min(resume_from, len(planned)))
    companies = planned[start_idx:]
    stats["selected"] = len(planned)

    for i, company in enumerate(companies, start=start_idx + 1):
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


# ── Industry re-derivation batch ─────────────────────────────────────────────

def rederive_industry_batch(
    db: Session,
    *,
    canton: str | None = None,
    industry_filter: str | None = None,
    min_zefix_score: int | None = None,
    limit: int | None = None,
    batch_size: int = 500,
    resume_from: int = 0,
    progress_cb: Any = None,
) -> dict[str, Any]:
    """Re-derive the industry label for matching companies using the current taxonomy.

    Uses best-match scoring (highest keyword hit count wins) and the configurable
    taxonomy from app_settings.  Re-computes zefix_score when the industry changes.

    Returns:
        ``{"updated": int, "unchanged": int, "errors": list[str]}``
    """
    stats: dict[str, Any] = {"updated": 0, "unchanged": 0, "errors": []}
    taxonomy = _load_industry_taxonomy(db)
    scoring_config = _load_scoring_config(db)

    query = db.query(Company)
    if canton:
        query = query.filter(Company.canton == canton)
    if industry_filter:
        query = query.filter(Company.industry.ilike(f"%{industry_filter}%"))
    if min_zefix_score is not None:
        query = query.filter(Company.zefix_score >= min_zefix_score)
    query = query.order_by(Company.id.asc())
    if limit:
        query = query.limit(limit)

    company_ids = [c.id for c in query.with_entities(Company.id).all()]
    total = len(company_ids)
    offset = max(0, min(resume_from, total))

    while offset < total:
        batch_ids = company_ids[offset: offset + batch_size]
        batch = db.query(Company).filter(Company.id.in_(batch_ids)).order_by(Company.id.asc()).all()

        for company in batch:
            try:
                new_industry = _derive_industry(company.purpose, taxonomy)
                if new_industry != company.industry:
                    company.industry = new_industry
                    score_breakdown = compute_zefix_score_breakdown(
                        legal_form=company.legal_form,
                        legal_form_short_name=company.legal_form_short_name,
                        capital_nominal=company.capital_nominal,
                        purpose=company.purpose,
                        branch_offices=company.branch_offices,
                        industry=new_industry,
                        status=company.status,
                        canton=company.canton,
                        municipality=company.municipality,
                        lat=company.lat,
                        lon=company.lon,
                        config=scoring_config,
                    )
                    company.zefix_score = int(score_breakdown["final_score"])
                    company.zefix_score_breakdown = json.dumps(score_breakdown)
                    stats["updated"] += 1
                else:
                    stats["unchanged"] += 1
            except Exception as exc:  # noqa: BLE001
                stats["errors"].append(f"{company.uid}: {exc}")

        db.commit()
        offset += len(batch)

        if progress_cb:
            progress_cb(min(offset, total), total, stats)

    return stats


# ── TF-IDF clustering batch ───────────────────────────────────────────────────

def tfidf_classify_batch(
    db: Session,
    *,
    canton: str | None = None,
    industry_filter: str | None = None,
    min_zefix_score: int | None = None,
    max_zefix_score: int | None = None,
    limit: int = 1000,
    n_clusters: int = 10,
    resume_from: int = 0,
    progress_cb: Any = None,
) -> dict[str, Any]:
    """Cluster companies by TF-IDF similarity of their purpose text.

    Assigns a human-readable cluster label (top-3 TF-IDF terms) to each
    company's ``tfidf_cluster`` field.  Uses K-Means on scikit-learn TF-IDF
    vectors; no external API calls.

    Returns:
        ``{"classified": int, "skipped": int, "errors": list[str]}``
    """
    try:
        from sklearn.cluster import KMeans
        from sklearn.feature_extraction.text import TfidfVectorizer
    except ImportError:
        return {"classified": 0, "skipped": 0, "errors": ["scikit-learn not installed. Run: pip install scikit-learn"]}

    stats: dict[str, Any] = {"classified": 0, "skipped": 0, "errors": []}

    query = db.query(Company).filter(Company.purpose.isnot(None))
    if canton:
        query = query.filter(Company.canton == canton)
    if industry_filter:
        query = query.filter(Company.industry.ilike(f"%{industry_filter}%"))
    if min_zefix_score is not None:
        query = query.filter(Company.zefix_score >= min_zefix_score)
    if max_zefix_score is not None:
        query = query.filter(Company.zefix_score <= max_zefix_score)

    companies = query.order_by(Company.id.asc()).limit(limit).all()
    if not companies:
        return stats

    purposes = [c.purpose or "" for c in companies]
    actual_k = min(n_clusters, len(companies))

    vectorizer = TfidfVectorizer(
        max_features=800,
        ngram_range=(1, 2),
        stop_words=list(_TFIDF_STOPWORDS),
        min_df=2,
        sublinear_tf=True,
    )
    try:
        X = vectorizer.fit_transform(purposes)
    except ValueError as exc:
        return {"classified": 0, "skipped": len(companies), "errors": [f"TF-IDF failed: {exc}"]}

    km = KMeans(n_clusters=actual_k, random_state=42, n_init=10)
    labels = km.fit_predict(X)

    # Build human-readable label for each cluster (top-3 TF-IDF terms by centroid weight)
    feature_names = vectorizer.get_feature_names_out()
    cluster_labels: dict[int, str] = {}
    for i in range(actual_k):
        center = km.cluster_centers_[i]
        top_idx = center.argsort()[-3:][::-1]
        top_terms = [feature_names[j] for j in top_idx]
        cluster_labels[i] = " · ".join(top_terms)

    total = len(companies)
    batch_size = 200
    offset = max(0, min(resume_from, total))

    for idx in range(offset, total, batch_size):
        batch_slice = slice(idx, idx + batch_size)
        for company, label_idx in zip(companies[batch_slice], labels[batch_slice]):
            try:
                company.tfidf_cluster = cluster_labels[int(label_idx)]
                stats["classified"] += 1
            except Exception as exc:  # noqa: BLE001
                stats["errors"].append(f"{company.uid}: {exc}")
                stats["skipped"] += 1
        db.commit()
        if progress_cb:
            progress_cb(min(idx + batch_size, total), total, stats)

    return stats


# ── Claude Haiku classification batch ────────────────────────────────────────

def claude_classify_batch(
    db: Session,
    *,
    canton: str | None = None,
    industry_filter: str | None = None,
    min_zefix_score: int | None = None,
    max_zefix_score: int | None = None,
    limit: int = 500,
    system_prompt: str | None = None,
    api_key: str | None = None,
    resume_from: int = 0,
    progress_cb: Any = None,
) -> dict[str, Any]:
    """Classify companies using Claude Haiku and store a lead-quality score + category.

    Each company's ``purpose`` (and optionally ``industry``) is sent to
    claude-haiku-4-5 with a classification prompt.  The response JSON is parsed
    to extract ``score`` (0-100) and ``category`` (short label), which are stored
    in ``claude_score`` and ``claude_category``.

    Returns:
        ``{"classified": int, "skipped": int, "errors": list[str], "input_tokens": int, "output_tokens": int}``
    """
    try:
        import anthropic as _anthropic
    except ImportError:
        return {"classified": 0, "skipped": 0, "errors": ["anthropic package not installed. Run: pip install anthropic"], "input_tokens": 0, "output_tokens": 0}

    if not api_key:
        return {"classified": 0, "skipped": 0, "errors": ["Anthropic API key not configured. Set ANTHROPIC_API_KEY in .env"], "input_tokens": 0, "output_tokens": 0}

    stats: dict[str, Any] = {"classified": 0, "skipped": 0, "errors": [], "input_tokens": 0, "output_tokens": 0}
    client = _anthropic.Anthropic(api_key=api_key)
    prompt = (system_prompt or "").strip() or _DEFAULT_CLAUDE_PROMPT

    query = db.query(Company).filter(Company.purpose.isnot(None))
    if canton:
        query = query.filter(Company.canton == canton)
    if industry_filter:
        query = query.filter(Company.industry.ilike(f"%{industry_filter}%"))
    if min_zefix_score is not None:
        query = query.filter(Company.zefix_score >= min_zefix_score)
    if max_zefix_score is not None:
        query = query.filter(Company.zefix_score <= max_zefix_score)

    companies = query.order_by(Company.id.asc()).limit(limit).all()
    total = len(companies)
    offset = max(0, min(resume_from, total))

    for i, company in enumerate(companies[offset:], start=offset + 1):
        try:
            user_text = f"Company: {company.name}\nPurpose: {company.purpose}"
            if company.industry:
                user_text += f"\nIndustry: {company.industry}"

            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=128,
                system=prompt,
                messages=[{"role": "user", "content": user_text}],
            )
            response_text = msg.content[0].text.strip()
            stats["input_tokens"] += msg.usage.input_tokens
            stats["output_tokens"] += msg.usage.output_tokens

            # Strip optional markdown code fences
            if response_text.startswith("```"):
                response_text = response_text.split("```")[1]
                if response_text.startswith("json"):
                    response_text = response_text[4:]

            data = json.loads(response_text)
            company.claude_score = max(0, min(100, int(data.get("score", 0))))
            company.claude_category = str(data.get("category", ""))[:128]
            company.claude_scored_at = datetime.now(tz=timezone.utc)
            stats["classified"] += 1

        except Exception as exc:  # noqa: BLE001
            stats["errors"].append(f"{company.uid}: {exc}")
            stats["skipped"] += 1

        if i % 50 == 0:
            db.commit()
            if progress_cb:
                progress_cb(i, total, stats)

        time.sleep(0.15)  # ~6 req/s — well within Haiku rate limits

    db.commit()
    if progress_cb:
        progress_cb(total, total, stats)

    return stats
