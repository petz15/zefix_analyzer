import json
import re
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
    compute_zefix_score_breakdown,
    distance_to_muri_km,
    distance_to_origin_km,
    fallback_result_score,
    get_default_scoring_config,
    is_irrelevant_result,
    is_social_lead_domain,
    normalize_raw_scores,
    score_result,
)



# Stopwords for TF-IDF vectorization (German + French + Italian + English + Swiss registry boilerplate)
# Goal: remove words that appear in almost every company purpose and don't help distinguish clusters.
# Do NOT add words that mark meaningful categories (e.g. "handel", "entwicklung", "bau", "immobilien").
_TFIDF_STOPWORDS: set[str] = {

    # Generic activity words — too broad to form meaningful clusters
    "erbringung", "dienstleistungen", "dienstleistung", "leistungen", "leistung",
    "waren", "ware",
    "tätigkeiten", "tätigkeit", "aktivitäten", "aktivität",
    "verwaltung", "führung", "betreuung",
    "bereich", "bereiche", "bereichen", "gebiet", "gebiete", "gebieten",
    "erwerb", "erwerben", "veräusserung", "veräussern",
    "beteiligung", "beteiligungen", "beteiligen", "halten", "verwalten", "betreiben",
    "erbringen", "anbieten", "durchführen", "ausführen",
    "ausführung", "ausführungen",
    "art",
    "übernahme", "übernahmen",
    "vertretungen", "vertretung",
    "zubehör",
    "dazugehörig", "dazugehörigen", "dazugehörigem",
    "darlehen", "immaterialgüter", "immaterialgüt", "anderer", "zusammenhängen", "bezwecken", 
    "einschliesslich", "einschließlich", "einschl", "ähnliche", "ähnlichen", "weitere", "weiteren", "entsprechende",
    "jeglicher", "zusammenhängende", "zusammenhängenden", "zusammenhängendem", "weiterveräussern", 
    "dritter", "dritten", "dritter", "dritten", "dritter", "dritten", "weit",
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
    "etc", "usw", "inklusive", "inkl", "exklusive", "exkl", "anderen", "anderer", "anderes", 
    "beispielsweise", "z.B", "zB", "u.a", "ua", "namentlich", "hauptsächlich", "vorzugsweise",
    "allgemein", "allgemeine", "allgemeinen", "sonstige", "sonstigen",
    "eigen", "eigene", "eigenen", "ähnliche", "ähnlichen", "weitere", "weiteren", "entsprechende",

    # ── Swiss registry standard boilerplate sentence (verbatim filler) ────────
    "kann", "errichten", "anderen", "andern", "geschäfte", "geschäftstätigkeit", "geschäftstätigkeiten",
    "tätigen", "direkt", "indirekt", "ihrem", "zusammenhang", "stehen",
    "grundeigentum", "grundstück", "grundstücke", "belasten", "finanzierungen", "eigene", "fremde", "rechnung",
    "vornehmen", "garantien", "bürgschaften", "dritte", "eingehen",
    "tochtergesellschaft", "tochtergesellschaften",
    "zweigniederlassung", "zweigniederlassungen", "niederlassung", "niederlassungen",
    "inland", "ausland", "verbundenen",
    "liegenschaften", "liegenschaft",
    "fördern", "fördert", "förderung",
    "geeignet", "geeignete", "geeigneten",
    "gesellschaftszweck",
    "zwecksetzung",
    "gleicher", "gleiche", "gleichen",
    "ähnlicher",
    "unternehmungen",
    "ferner",
    "bezweckt",                                              # "Die Gesellschaft bezweckt:" intro
    "gleichartige", "gleichartiger", "gleichartigen",        # "gleichartige oder verwandte Unternehmen"
    "verwandte", "verwandten", "verwandter",                 # ibid
    "solchen",                                               # "sich mit solchen zusammenschliessen"
    "zusammenschliessen",                                    # "sich mit solchen zusammenschliessen"
    "verträge", "vertrag",                                   # "Verträge abschliessen"
    "abschliessen",                                          # ibid
    "sicherheiten",                                          # "Garantien und Sicherheiten zugunsten"
    "zugunsten",                                             # "zugunsten verbundener Gesellschaften"
    "gewähren",                                              # "Garantien und Sicherheiten gewähren"
    "übernehmen",                                            # "Finanzierungen übernehmen"
    "damit",                                                 # "direkt oder indirekt damit im Zusammenhang"
    "sämtliche", "sämtlichen",                              # synonym for "alle"
    "innmaterialgüterrechte",                               # IP rights boilerplate
    "unternehmens",                                          # genitive: "Zweck des Unternehmens"
    "fiduziare", "fiduziar", "fiduziaren",                        # "fiduziare Verwaltung von Vermögen Dritter"
    "übereignung", "übereignung", "übertragung", "übertragen",           # "Übertragung von Vermögen"
    "pfandrecht", "pfandrechte", "verpfändung", "verpfänden",                 # "Verpfändung von Vermögen"
    "mittels",
    "aktiven", "aktiven", "passiven", "passiven", "beteiligungen", "beteiligen",
    "aktionär", "aktionäre", "dritter", "dritten",
    "fremd", "finanzierung", "geschäft",
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

    # Custom sentences/words that appear in almost every purpose and don't help clustering
    "handel mit waren", "mit waren aller art", "erbringung von dienstleistungen", "dienstleistungen aller art", "handel mit waren aller art", 
    "fremd garantie", 
    "garantie bürgschaft","bürgschaft dritter",
    "bürgschaft",
    "garantie",
    "können",
    "anderer geschäft",
    "verbunden abgeben",
    "fremd sicherheit",
    "sicherheit verbindlichkeit",
    "verbindlichkeit verbunden",
    "abgeben",
    "verbindlichkeit",
    "verbunden",
    "übrig vgl",
    "übrig können",
    "übrig kommerziell",
    "übrig immaterialgüterrecht",
    "immaterialgüterrecht",
    "ausländisch",
    "übrig geschäft",
    "übrig finanzierung",
    "übrig",
    "vornahme finanzierung",
    "eingehung bürgschaft",
    "finanzierung eingehung",
    "vornahme",
    "eingehung",
    "belastung vornahme",
    "belastung",
    "genossenschaft",
    "anderer gleichartig",
    "verwandt zusammenschliess",
    "gleichartig verwandt",
    "zusammenschliess",
    "gleichartig",
    "verwandt",
    "wert vermitteln",
    "immateriell wert",
    "anderer immateriell",
    "wert",
    "überhaupt geschäft",
    "lizenz patent",
    "aufnehmen garantie",
    "darlehen aufnehmen",
    "garantie anderer",
    "stellen immaterialgüterrecht",
    "aufnehmen",
    "stiftung",
    "bezwecken handel",
    "kredit",
    "sicherheit",
    "gegenüber dritter",
    "verbindlichkeit gegenüber",
    "hauptzweck erzielen",
    "synergie hauptzweck",
    "synergie",
    "erzielen",
    "geschäft synergie",
    "hauptzweck",
    "erzielen können",
    "führen",
    "schutzrecht",
    "weiterveräussern geschäft",
    "unternehmung vorkehren",
    "vorkehren",
    "dienen",
    "dienen können",
    "anderer unternehmung",
    "unternehmung",
    "unternehmung gleichartig",
    "konzerngesellschaft dritter",
    "konzerngesellschaft",
    "aktionär konzerngesellschaft",
    "finanzierung sanierung",
    "verpflichtung sicherheit",
    "darlehen verpflichtung",
    "gunst",
    "anderer geschäft",
    "fiduziarisch jeglicher", "geschäft entwickeln",


    # ── Holding/intercompany boilerplate (Cash-Pooling, interco financing) ────
    "überdies",                                          # "Die Gesellschaft kann überdies..."
    "entgeltlich", "unentgeltlich",                      # "entgeltlich oder unentgeltlich"
    "personen", "person",                                # "Verbindlichkeiten solcher Personen"
    "zudem",                                             # "Zudem kann sie..."
    "daran",                                             # "sich daran beteiligen"
    "zwar",                                              # "und zwar auch ohne Gegenleistung"
    "ohne",                                              # "ohne Gegenleistung"
    "gegenleistung",                                     # "ohne Gegenleistung"
    "zinslos",                                           # "zinslos" financing
    "ausschluss",                                        # "unter Ausschluss der Gewinnerzielungsabsicht"
    "gewinnerzielungsabsicht",                           # ibid
    "klumpenrisiko", "klumpenrisiken",                   # "unter Übernahme von Klumpenrisiken"
    "gruppengesellschaft", "gruppengesellschaften",      # "mit Gruppengesellschaften"
    "liquiditätsausgleich", "liquiditätsausgleiche",     # Cash-pooling synonym
    "nettoliquiditätszentralisierung", "nettoliquiditätszentralisierungen",  # ibid
    "cashpooling", "cash-pooling",                       # "(Cash-Pooling)"
    "periodisch", "periodische", "periodischer", "periodischen",  # "periodischer Saldoanpassungen"
    "saldoanpassung", "saldoanpassungen",                # "(Balancing)"
    "balancing",                                         # English term in German text
    "vorzugskondition", "vorzugskonditionen",            # "zu Vorzugskonditionen"
    "kommerziell", "kommerzielle", "kommerziellen",      # "kommerziellen Geschäfte"
    "finanziell", "finanzielle", "finanziellen",         # "finanziellen Geschäfte"
    "tätigen",                                           # "Geschäfte tätigen" — too generic
}

_SENTENCE_SPLIT = re.compile(r'(?<=\.)\s+(?=[A-ZÄÖÜ])')


def strip_purpose_boilerplate(text: str, patterns: list[re.Pattern]) -> str:
    """Remove boilerplate sentences from purpose text using DB-loaded patterns.

    Splits on sentence boundaries (period + capital letter), drops any sentence
    matching a pattern, and rejoins the rest.  Falls back to the original text
    if everything would be stripped.
    """
    if not text or len(text) < 40 or not patterns:
        return text

    sentences = _SENTENCE_SPLIT.split(text.strip())
    kept = [
        s for s in sentences
        if s.strip() and not any(pat.search(s) for pat in patterns)
    ]
    result = " ".join(kept).strip()
    return result if result else text


# Default system prompt for Claude classification
_DEFAULT_CLAUDE_PROMPT = """\
You are evaluating Swiss company register (Zefix) entries as B2B sales leads.

Each entry includes the company name, purpose text, \
purpose keywords (TF-IDF terms from the company's own text), cluster labels \
(segment groups derived from similar companies), and optionally a website URL \
and Google score (0–100 confidence that the found website belongs to this company).

Use ALL provided fields together. The keywords and cluster labels are pre-computed \
hints — let the purpose text be the primary signal when they conflict. \
A high Google score (≥60) suggests the company has an active, findable web presence \
which is a mild positive signal. A missing website or low Google score is neutral — \
many legitimate SMEs are not indexed.

Output ONLY a JSON object (no markdown, no explanation) with exactly two fields:
- "score": integer 0–100
- "category": short English label (e.g. "SaaS", "Industrial Machinery", "Accounting", "E-Commerce")

Scoring guidance — spread scores across the full range:
- 85–100: Strong lead. Active SME clearly operating in the target space.
- 60–84:  Relevant but not ideal. Adjacent industry or mixed activities.
- 35–59:  Marginal. Some overlap but mostly outside the target.
- 10–34:  Weak. Different industry, very generic, or unclear purpose.
- 0–9:    Irrelevant. Holding company, dormant entity, non-commercial association.

Do NOT cluster scores in the 40–70 band. Use the full range so leads can be ranked meaningfully.\
"""


def _load_scoring_config(db: Session) -> dict[str, str]:
    defaults = get_default_scoring_config()
    return {key: crud.get_setting(db, key, val) for key, val in defaults.items()}


def _extract_company_fields(
    raw: dict[str, Any],
    fallback_uid: str,
    *,
    scoring_config: dict[str, str] | None = None,
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

    # Extract purpose from multilingual dict if needed
    purpose_raw = raw.get("purpose") or raw.get("purposes") or None
    if isinstance(purpose_raw, list):
        purpose = " ".join(str(p) for p in purpose_raw if p) or None
    elif isinstance(purpose_raw, dict):
        purpose = (
            purpose_raw.get("de") or purpose_raw.get("fr")
            or purpose_raw.get("it") or purpose_raw.get("en")
            or next(iter(purpose_raw.values()), None) or None
        )
    else:
        purpose = str(purpose_raw) if purpose_raw else None

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

    score_breakdown = compute_zefix_score_breakdown(
        legal_form=legal_form_display,
        legal_form_short_name=legal_form_short,
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
    company_data = _extract_company_fields(raw, uid, scoring_config=scoring_config)

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
        status=company.status,
        canton=company.canton,
        municipality=company.municipality,
        lat=lat,
        lon=lon,
        purpose_keywords=company.purpose_keywords,
        tfidf_cluster=company.tfidf_cluster,
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
    """Recompute and normalise zefix_score for every company.

    Two-pass approach:
      1. Compute raw scores for all companies (geocoding if needed).
      2. Min-max normalise non-cancelled scores to 0-100; cancelled → cancelled_score.

    Returns:
        ``{"updated": int, "geocoded": int, "errors": list[str]}``
    """
    stats: dict[str, Any] = {"updated": 0, "geocoded": 0, "errors": []}
    scoring_config = _load_scoring_config(db)
    cancelled_score = int(scoring_config.get("scoring_cancelled_score", "5"))

    # ── Pass 1: compute raw scores ────────────────────────────────────────────
    raw_scores: dict[int, int | None] = {}   # company.id → raw_total or None if cancelled
    breakdowns: dict[int, dict] = {}

    total = db.query(Company).count()
    offset = max(0, min(resume_from, total))

    while True:
        batch = db.query(Company).order_by(Company.id.asc()).offset(offset).limit(batch_size).all()
        if not batch:
            break
        for company in batch:
            try:
                if company.lat is None and company.lon is None and company.address:
                    coords = geocode_address(company.address)
                    if coords:
                        company.lat, company.lon = coords
                        stats["geocoded"] += 1
                bd = compute_zefix_score_breakdown(
                    legal_form=company.legal_form,
                    legal_form_short_name=company.legal_form_short_name,
                    status=company.status,
                    canton=company.canton,
                    municipality=company.municipality,
                    lat=company.lat,
                    lon=company.lon,
                    purpose_keywords=company.purpose_keywords,
                    tfidf_cluster=company.tfidf_cluster,
                    config=scoring_config,
                )
                breakdowns[company.id] = bd
                raw_scores[company.id] = None if bd.get("cancelled") else int(bd["raw_total"])
            except Exception as exc:  # noqa: BLE001
                stats["errors"].append(f"{company.uid}: {exc}")
        db.commit()
        offset += len(batch)
        if progress_cb:
            progress_cb(min(offset, total), total, stats)

    # ── Pass 2: normalise and write final scores ───────────────────────────────
    normalised = normalize_raw_scores(raw_scores, cancelled_score=cancelled_score)

    offset = 0
    while True:
        batch = db.query(Company).order_by(Company.id.asc()).offset(offset).limit(batch_size).all()
        if not batch:
            break
        for company in batch:
            if company.id not in normalised:
                continue
            bd = breakdowns.get(company.id, {})
            bd["final_score"] = normalised[company.id]
            company.zefix_score = normalised[company.id]
            company.zefix_score_breakdown = json.dumps(bd)
            stats["updated"] += 1
        db.commit()
        offset += len(batch)

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
                address=company.address,
            )
        else:
            s = score_result(
                row,
                company_name=company.name,
                municipality=company.municipality,
                canton=company.canton,
                purpose=company.purpose,
                legal_form=company.legal_form,
                address=company.address,
            )

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
        batch = (
            db.query(Company)
            .order_by(Company.zefix_score.desc().nullslast(), Company.id.asc())
            .offset(offset)
            .limit(batch_size)
            .all()
        )
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
                company.social_media_only = is_social_lead_domain(best["link"])
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
    social_media_only = is_social_lead_domain(best["link"])

    crud.update_company(
        db,
        company,
        CompanyUpdate(
            website_url=best["link"],
            website_match_score=best["score"],
            social_media_only=social_media_only,
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
        "status", "municipality", "canton", "purpose", "zefix_score", "zefix_score_breakdown",
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
                _score_bd = compute_zefix_score_breakdown(
                    legal_form=result.legal_form,
                    legal_form_short_name=result.legal_form_short_name,
                    status=result.status,
                    canton=result.canton or canton,
                    municipality=result.municipality,
                    config=scoring_config,
                )
                company_data = CompanyCreate(
                    uid=result.uid,
                    name=result.name,
                    legal_form=result.legal_form,
                    legal_form_id=result.legal_form_id,
                    legal_form_uid=result.legal_form_uid,
                    legal_form_short_name=result.legal_form_short_name,
                    status=result.status,
                    municipality=result.municipality,
                    canton=result.canton or canton,
                    purpose=result.purpose,
                    zefix_score=int(_score_bd["final_score"]),
                    zefix_score_breakdown=json.dumps(_score_bd),
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
            social_media_only=is_social_lead_domain(best["link"]),
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




# ── Claude Haiku classification batch ────────────────────────────────────────

def claude_classify_batch(
    db: Session,
    *,
    canton: str | None = None,
    min_zefix_score: int | None = None,
    max_zefix_score: int | None = None,
    limit: int = 500,
    system_prompt: str | None = None,
    target_description: str | None = None,
    api_key: str | None = None,
    resume_from: int = 0,
    use_batch_api: bool = False,
    companies_per_message: int = 1,
    progress_cb: Any = None,
) -> dict[str, Any]:
    """Classify companies using Claude Haiku and store a lead-quality score + category.

    ``companies_per_message`` controls how many companies are packed into a single
    API call (default 1).  Higher values (e.g. 10–20) amortise the system prompt
    across multiple companies, significantly reducing input token costs.  The
    system prompt is also always sent with ``cache_control: ephemeral`` so that
    repeated identical prompts are served from cache at ~10 % of normal cost.

    ``use_batch_api=True`` submits all requests as a single Anthropic Message Batch
    (50 % discount on top).  The call blocks while polling for completion.

    Returns:
        ``{"classified": int, "skipped": int, "errors": list[str],
           "input_tokens": int, "output_tokens": int}``
        Batch mode also includes ``"batch_id": str``.
    """
    try:
        import anthropic as _anthropic
    except ImportError:
        return {"classified": 0, "skipped": 0, "errors": ["anthropic package not installed. Run: pip install anthropic"], "input_tokens": 0, "output_tokens": 0}

    if not api_key:
        return {"classified": 0, "skipped": 0, "errors": ["Anthropic API key not configured. Set ANTHROPIC_API_KEY in .env"], "input_tokens": 0, "output_tokens": 0}

    stats: dict[str, Any] = {"classified": 0, "skipped": 0, "errors": [], "input_tokens": 0, "output_tokens": 0}
    client = _anthropic.Anthropic(api_key=api_key)
    base_prompt = (system_prompt or "").strip() or _DEFAULT_CLAUDE_PROMPT
    if target_description and target_description.strip():
        prompt = base_prompt + f"\n\nWhat we are looking for: {target_description.strip()}"
    else:
        prompt = base_prompt

    companies_per_message = max(1, companies_per_message)

    # When sending multiple companies per message, append the array-output instruction.
    if companies_per_message > 1:
        prompt = (
            prompt
            + '\n\nYou will receive multiple companies separated by "---".'
            + " Output ONLY a JSON array with one object per company in the same order,"
            + ' each with "score" (0-100) and "category".'
        )

    # Wrap system in list with cache_control so the prompt is cached after the first
    # call — subsequent calls pay ~10% of normal input token price for the cached prefix.
    system_param: list[dict] = [{"type": "text", "text": prompt, "cache_control": {"type": "ephemeral"}}]

    scoring_config = _load_scoring_config(db)
    origin_lat = float(scoring_config.get("scoring_origin_lat", 46.9266))
    origin_lon = float(scoring_config.get("scoring_origin_lon", 7.4817))

    query = db.query(Company).filter(Company.purpose.isnot(None))
    if canton:
        query = query.filter(Company.canton == canton)
    if min_zefix_score is not None:
        query = query.filter(Company.zefix_score >= min_zefix_score)
    if max_zefix_score is not None:
        query = query.filter(Company.zefix_score <= max_zefix_score)

    all_candidates = query.all()
    all_candidates.sort(key=lambda c: (
        -(c.zefix_score if c.zefix_score is not None else -1),
        distance_to_origin_km(origin_lat, origin_lon, canton=c.canton, municipality=c.municipality, lat=c.lat, lon=c.lon) or float("inf"),
        c.id,
    ))
    companies = all_candidates[:limit]
    total = len(companies)
    offset = max(0, min(resume_from, total))
    selected = companies[offset:]

    # ── helpers ──────────────────────────────────────────────────────────────

    # Load active boilerplate patterns from DB once for the whole run
    _boilerplate_patterns = crud.get_active_boilerplate_patterns(db)

    def _build_user_text(company: Company) -> str:
        purpose = strip_purpose_boilerplate(company.purpose or "", _boilerplate_patterns)
        parts = [f"Company: {company.name}", f"Purpose: {purpose}"]
        if company.purpose_keywords:
            parts.append(f"Keywords: {company.purpose_keywords}")
        if company.tfidf_cluster and company.tfidf_cluster != "Undefined":
            parts.append(f"Clusters: {company.tfidf_cluster}")
        if company.website_url:
            parts.append(f"Website: {company.website_url}")
        if company.website_match_score is not None:
            parts.append(f"Google score: {company.website_match_score} (0–100 confidence that the website belongs to this company)")
        if company.social_media_only:
            parts.append("Note: only social media presence found — no company website")
        return "\n".join(parts)

    def _strip_fences(text: str) -> str:
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return text.strip()

    def _apply_single(company: Company, response_text: str) -> None:
        data = json.loads(_strip_fences(response_text))
        company.claude_score = max(0, min(100, int(data.get("score", 0))))
        company.claude_category = str(data.get("category", ""))[:128]
        company.claude_scored_at = datetime.now(tz=timezone.utc)

    def _apply_chunk(chunk: list[Company], response_text: str) -> None:
        data = json.loads(_strip_fences(response_text))
        if not isinstance(data, list):
            raise ValueError(f"Expected JSON array, got {type(data).__name__}: {response_text!r}")
        now = datetime.now(tz=timezone.utc)
        for company, item in zip(chunk, data):
            company.claude_score = max(0, min(100, int(item.get("score", 0))))
            company.claude_category = str(item.get("category", ""))[:128]
            company.claude_scored_at = now

    def _chunk_ids(chunk: list[Company]) -> str:
        return ", ".join(c.uid for c in chunk)

    # Group selected companies into chunks of `companies_per_message`
    chunks: list[list[Company]] = [
        selected[i: i + companies_per_message]
        for i in range(0, len(selected), companies_per_message)
    ]

    # ── Batch API path ────────────────────────────────────────────────────────

    if use_batch_api:
        def _batch_custom_id(idx: int, chunk: list[Company]) -> str:
            if len(chunk) == 1:
                return chunk[0].uid.replace(".", "_")[:64]
            return f"chunk_{idx}"

        chunk_map: dict[str, list[Company]] = {}
        requests_list = []
        for idx, chunk in enumerate(chunks):
            cid = _batch_custom_id(idx, chunk)
            chunk_map[cid] = chunk
            content = _build_user_text(chunk[0]) if len(chunk) == 1 else "\n---\n".join(_build_user_text(c) for c in chunk)
            requests_list.append({
                "custom_id": cid,
                "params": {
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 128 * len(chunk),
                    "system": system_param,
                    "messages": [{"role": "user", "content": content}],
                },
            })

        if not requests_list:
            return stats

        batch = client.beta.messages.batches.create(requests=requests_list)
        stats["batch_id"] = batch.id

        if progress_cb:
            progress_cb(0, total, stats)

        while batch.processing_status != "ended":
            time.sleep(15)
            batch = client.beta.messages.batches.retrieve(batch.id)
            counts = batch.request_counts
            done_so_far = (counts.succeeded or 0) + (counts.errored or 0)
            if progress_cb:
                progress_cb(min(done_so_far * companies_per_message, total), total, stats)

        for result in client.beta.messages.batches.results(batch.id):
            chunk = chunk_map.get(result.custom_id)
            if chunk is None:
                continue
            if result.result.type == "succeeded":
                response_text = result.result.message.content[0].text.strip()
                stats["input_tokens"] += result.result.message.usage.input_tokens
                stats["output_tokens"] += result.result.message.usage.output_tokens
                try:
                    if len(chunk) == 1:
                        _apply_single(chunk[0], response_text)
                    else:
                        _apply_chunk(chunk, response_text)
                    stats["classified"] += len(chunk)
                except json.JSONDecodeError as exc:
                    stats["errors"].append(f"{_chunk_ids(chunk)}: JSON parse error — raw: {response_text!r} — {exc}")
                    stats["skipped"] += len(chunk)
                except Exception as exc:  # noqa: BLE001
                    stats["errors"].append(f"{_chunk_ids(chunk)}: {type(exc).__name__}: {exc}")
                    stats["skipped"] += len(chunk)
            else:
                err_info = getattr(result.result, "error", result.result.type)
                stats["errors"].append(f"{_chunk_ids(chunk)}: batch error — {err_info}")
                stats["skipped"] += len(chunk)

        db.commit()
        if progress_cb:
            progress_cb(total, total, stats)
        return stats

    # ── Per-request path (default) ────────────────────────────────────────────

    for i, chunk in enumerate(chunks):
        done = offset + i * companies_per_message + len(chunk)
        try:
            content = _build_user_text(chunk[0]) if len(chunk) == 1 else "\n---\n".join(_build_user_text(c) for c in chunk)
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=128 * len(chunk),
                system=system_param,
                messages=[{"role": "user", "content": content}],
            )
            response_text = msg.content[0].text.strip()
            stats["input_tokens"] += msg.usage.input_tokens
            stats["output_tokens"] += msg.usage.output_tokens
            if len(chunk) == 1:
                _apply_single(chunk[0], response_text)
            else:
                _apply_chunk(chunk, response_text)
            stats["classified"] += len(chunk)

        except json.JSONDecodeError as exc:
            stats["errors"].append(f"{_chunk_ids(chunk)}: JSON parse error — raw: {response_text!r} — {exc}")
            stats["skipped"] += len(chunk)
        except Exception as exc:  # noqa: BLE001
            stats["errors"].append(f"{_chunk_ids(chunk)}: {type(exc).__name__}: {exc}")
            stats["skipped"] += len(chunk)

        if done % 50 < companies_per_message or done >= total:
            db.commit()
            if progress_cb:
                progress_cb(min(done, total), total, stats)

        time.sleep(0.15)

    db.commit()
    if progress_cb:
        progress_cb(total, total, stats)

    return stats
