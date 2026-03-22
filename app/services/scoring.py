"""Scoring logic for matching Google Search results to a company profile."""

import math
import re
from urllib.parse import urlparse

# Domains that are business directories, social networks, or government registries.
_DIRECTORY_DOMAINS = {
    "wikipedia.org",
    "zefix.admin.ch",
    "uid.admin.ch",
    "moneyhouse.ch",
    "shab.ch",
    "search.ch",
    "yelp.com",
    "local.ch",
    "yellowpages.ch",
    "directories.ch",
    "scout24.ch",
    "homegate.ch",
    "companyhouse.ch",
    "handelsregister.ch",
    "hr-register.ch",
    "rocketreach.co",
    "kununu.com",
    "crunchbase.com",
    "rocketreach.com",
    "tiger.ch",
    "help.ch",
    "kompass.ch",
    "spheriq.ch",
    "treuhandsuisse.ch",
    "fiduciairesuisse-vd.ch",
    "business-monitor.ch",
    "graph.swiss",
    "swiss-arc.ch",
    "kompass.com",
    "northdata.com",
    "northdata.de",
    "northdata.eu",
    "northdata.ch",
    "provenexpert.com",
    "bestatter1.ch",
    "die-bestatter.ch",
    "auditorstats.ch",
    "maptons.com",
    "pappers.ch",
    "kanzleiwelten.com",
    "yellowpages.swiss",
    "yellowpages.ch",
    "yellowpages.com",
    "lixt.com",
    "swissbiotech.org",
    "ofri.ch",
    "region-emmental.ch",
    "bloomberg.com",
    "yandex.ru",
    "autolina.ch",
    "autoscout24.ch",
    "comparis.ch",
}

_NEWS_DOMAINS = {
    "news.google.com",
    "20min.ch",
    "gastrojournal.ch",
    "nzz.ch",
    "srf.ch",
    "swissinfo.ch",
    "blick.ch",
    "aargauerzeitung.ch",
    "bernerzeitung.ch",
    "derbund.ch",
    "tagesanzeiger.ch",
    "luzernerzeitung.ch",
    "stgallerzeitung.ch",
    "suedostschweiz.ch",
    "nau.ch",
    "watson.ch",
}

_SOCIAL_LEAD_DOMAINS = {
    "linkedin.com",
    "facebook.com",
    "twitter.com",
    "x.com",
    "xing.com",
    "instagram.com",
    "youtube.com",
    "tiktok.com",
}

# Legal form words excluded when matching company name against domain
_LEGAL_FORM_WORDS = {"ag", "gmbh", "sa", "sarl", "sàrl", "kg", "og", "llc", "ltd", "inc", "co", "spa"}

# URL path pattern for municipal/local company directories (not in _DIRECTORY_DOMAINS).
# Matches pages like /unternehmensverzeichnis/, /firmenverzeichnis/, /verzeichnis/, etc.
_LOCAL_DIRECTORY_PATH_RE = re.compile(
    r"(?:unternehmens|firmen|branchen|betriebs)?verzeichnis", re.IGNORECASE
)

# Words to exclude when extracting keywords from the purpose field
_STOPWORDS = {
    "die", "der", "das", "und", "oder", "mit", "von", "für", "des", "dem",
    "den", "ein", "eine", "einer", "eines", "sich", "auf", "zu", "ist",
    "sowie", "als", "auch", "nicht", "nach", "bei", "alle", "durch", "wird",
    "the", "and", "of", "in", "for", "to", "a", "an", "with", "its",
    "gesellschaft", "unternehmen", "betrieb", "zweck", "aktien", "gmbh",
}


def _word_overlap_ratio(a: str, b: str) -> float:
    """Fraction of words in *a* that appear in *b* (case-insensitive)."""
    words_a = set(re.findall(r"\w+", a.lower()))
    words_b = set(re.findall(r"\w+", b.lower()))
    if not words_a:
        return 0.0
    return len(words_a & words_b) / len(words_a)


def _domain_name_overlap(domain: str, company_name: str) -> float:
    """Fraction of meaningful company name words found in the domain string.

    Uses substring containment so concatenated domains match correctly:
    "aarestadt" and "gastro" are both substrings of "aarestadtgastro.ch" → 1.0.
    Strips legal form suffixes (AG, GmbH, …) and short tokens before comparing.
    """
    domain_lower = domain.lower()
    name_words = [
        w for w in re.findall(r"\w+", company_name.lower())
        if len(w) >= 3 and w not in _LEGAL_FORM_WORDS
    ]
    if not name_words:
        return 0.0
    hits = sum(1 for w in name_words if w in domain_lower)
    return hits / len(name_words)


def _root_domain(url: str) -> str:
    try:
        netloc = urlparse(url).netloc.lower()
        return netloc.removeprefix("www.")
    except Exception:
        return ""


def _purpose_keywords(purpose: str | None, max_keywords: int = 8) -> list[str]:
    """Extract meaningful content words from a company's purpose text."""
    if not purpose:
        return []
    words = re.findall(r"\b[a-zA-ZäöüÄÖÜ]{4,}\b", purpose.lower())
    return [w for w in words if w not in _STOPWORDS][:max_keywords]


def _extract_address_parts(address: str) -> tuple[str | None, str | None]:
    """Return (zip_code, street_name) extracted from a Swiss address string.

    zip_code  — first 4-digit sequence found (e.g. "3001")
    street_name — leading alphabetic word(s) before the first street number,
                  lowercased (e.g. "musterstrasse", "rue du moulin").
    Returns None for each part that cannot be extracted.
    """
    zip_code = None
    street_name = None

    zip_match = re.search(r"\b(\d{4})\b", address)
    if zip_match:
        zip_code = zip_match.group(1)

    # Street name: take the run of non-digit words at the start of the address
    # (stops at the first digit, e.g. house number)
    street_match = re.match(r"^([^\d,]+?)(?:\s+\d|\s*,|$)", address.strip())
    if street_match:
        candidate = street_match.group(1).strip().lower()
        if len(candidate) >= 5:  # ignore very short tokens (noise)
            street_name = candidate

    return zip_code, street_name


def score_result(
    result: dict,
    *,
    company_name: str,
    municipality: str | None,
    canton: str | None,
    purpose: str | None = None,
    legal_form: str | None = None,
    address: str | None = None,
) -> int:
    """Score a single Google search result against a company profile.

    Returns an integer 0-100.

    Breakdown:
      - Name match in title:        0-30 pts  (word overlap × 30)
      - Name match in snippet:      0-20 pts  (word overlap × 20)
      - Domain name matches company: 0-15 pts  (overlap of company words in domain)
      - Location in combined text:  0-65 pts  (municipality 25 + canton 10
                                               + zip 15 + street 15)
      - Purpose keywords in snippet: 0-15 pts  (1-2 hits = 8, 3+ hits = 15)
      - Legal form in domain/title:   +5 pts  bonus
      - Swiss TLD (.ch / .swiss):    +10 pts  bonus
      - Social media domain:         -30 pts  penalty
      - Local directory URL path:   hard  0  (verzeichnis in URL path)
      - Directory domain:           hard  0  (returned immediately)
    """
    title = result.get("title", "") or ""
    snippet = result.get("snippet", "") or ""
    link = result.get("link", "") or ""

    # --- Directory / news domain → always 0, no further scoring ---
    domain = _root_domain(link)
    if any(domain == d or domain.endswith("." + d) for d in _DIRECTORY_DOMAINS | _NEWS_DOMAINS):
        return 0

    # --- Local directory URL path → always 0 (e.g. /unternehmensverzeichnis/) ---
    if _LOCAL_DIRECTORY_PATH_RE.search(link):
        return 0

    combined_lower = f"{title} {snippet}".lower()
    snippet_lower = snippet.lower()

    # --- Name in title (0-30) ---
    score = int(_word_overlap_ratio(company_name, title) * 30)

    # --- Name in snippet (0-20) ---
    score += int(_word_overlap_ratio(company_name, snippet) * 20)

    # --- Domain name matches company name (0-15) ---
    score += int(_domain_name_overlap(domain, company_name) * 15)

    # --- Location match (0-65) ---
    if municipality and municipality.lower() in combined_lower:
        score += 25
    if canton and canton.upper() in f"{title} {snippet}".upper():
        score += 10
    if address:
        zip_code, street_name = _extract_address_parts(address)
        if zip_code and zip_code in f"{title} {snippet}":
            score += 15
        if street_name and street_name in combined_lower:
            score += 15

    # --- Purpose keywords in snippet (0-15) ---
    keywords = _purpose_keywords(purpose)
    if keywords:
        hits = sum(1 for kw in keywords if kw in snippet_lower)
        if hits >= 3:
            score += 15
        elif hits >= 1:
            score += 8

    # --- Legal form presence in domain or title (+5 bonus) ---
    if legal_form:
        lf_lower = legal_form.lower()
        abbrevs = re.findall(r"\b\w{2,6}\b", lf_lower)
        if any(a in domain or a in title.lower() for a in abbrevs if len(a) >= 2):
            score += 5

    # --- Swiss TLD bonus (+10) ---
    if domain.endswith(".ch") or domain.endswith(".swiss"):
        score += 10

    # --- Social media penalty (-30) ---
    if any(domain == d or domain.endswith("." + d) for d in _SOCIAL_LEAD_DOMAINS):
        score -= 30

    return max(0, min(100, score))


def is_irrelevant_result(
    result: dict,
    *,
    company_name: str,
) -> bool:
    """Return True when a search result is likely not the company's own website.

    Heuristics:
      - Directory/social/government registry domain, or
      - Very low company-name overlap in both title and snippet.
    """
    title = result.get("title", "") or ""
    snippet = result.get("snippet", "") or ""
    link = result.get("link", "") or ""

    domain = _root_domain(link)
    if any(domain == d or domain.endswith("." + d) for d in _DIRECTORY_DOMAINS | _NEWS_DOMAINS):
        return True

    title_overlap = _word_overlap_ratio(company_name, title)
    snippet_overlap = _word_overlap_ratio(company_name, snippet)
    return title_overlap < 0.2 and snippet_overlap < 0.2


def fallback_result_score(
    result: dict,
    *,
    municipality: str | None,
    canton: str | None,
    legal_form: str | None = None,
    address: str | None = None,
) -> int:
    """Fallback website score used when top results are mostly irrelevant.

    Formula: base 5 + location (municipality/canton/zip/street) + legal-form presence.
    """
    title = result.get("title", "") or ""
    snippet = result.get("snippet", "") or ""
    link = result.get("link", "") or ""

    # Directory / news domains must never be selected as the company website
    domain = _root_domain(link)
    if any(domain == d or domain.endswith("." + d) for d in _DIRECTORY_DOMAINS | _NEWS_DOMAINS):
        return 0

    combined = f"{title} {snippet}"
    combined_lower = combined.lower()

    score = 5

    if municipality and municipality.lower() in combined_lower:
        score += 25
    if canton and canton.upper() in combined.upper():
        score += 10
    if address:
        zip_code, street_name = _extract_address_parts(address)
        if zip_code and zip_code in combined:
            score += 15
        if street_name and street_name in combined_lower:
            score += 15

    if legal_form:
        lf_lower = legal_form.lower()
        abbrevs = re.findall(r"\b\w{2,6}\b", lf_lower)
        if any(a in domain or a in title.lower() for a in abbrevs if len(a) >= 2):
            score += 5

    return max(0, min(100, score))


def is_social_lead_domain(url: str) -> bool:
    """Return True when URL belongs to a social domain treated as high lead value."""
    domain = _root_domain(url)
    return any(domain == d or domain.endswith("." + d) for d in _SOCIAL_LEAD_DOMAINS)


# ── Location helpers ────────────────────────────────────────────────────────
# Default origin: Muri bei Bern (lat, lon) — used for distance_to_muri_km helper
_ORIGIN = (46.9266, 7.4817)

# Approximate coordinates of canton capitals — fallback when municipality not found
_CANTON_COORDS: dict[str, tuple[float, float]] = {
    "AG": (47.391, 8.044), "AI": (47.331, 9.410), "AR": (47.388, 9.275),
    "BE": (46.948, 7.447), "BL": (47.485, 7.736), "BS": (47.560, 7.589),
    "FR": (46.807, 7.162), "GE": (46.204, 6.143), "GL": (47.040, 9.068),
    "GR": (46.850, 9.533), "JU": (47.366, 7.344), "LU": (47.050, 8.309),
    "NE": (47.000, 6.933), "NW": (46.958, 8.366), "OW": (46.897, 8.247),
    "SG": (47.424, 9.377), "SH": (47.696, 8.634), "SO": (47.209, 7.538),
    "SZ": (47.021, 8.651), "TG": (47.558, 8.897), "TI": (46.004, 8.951),
    "UR": (46.881, 8.645), "VD": (46.520, 6.632), "VS": (46.232, 7.360),
    "ZG": (47.166, 8.515), "ZH": (47.377, 8.542),
}

# Key Swiss municipalities → (lat, lon).  Lower-cased for lookup.
_MUNICIPALITY_COORDS: dict[str, tuple[float, float]] = {
    "muri bei bern": (46.927, 7.482), "bern": (46.948, 7.447),
    "köniz": (46.921, 7.410), "ostermundigen": (46.957, 7.494),
    "ittigen": (46.974, 7.481), "worb": (46.928, 7.565),
    "münsingen": (46.874, 7.564), "belp": (46.891, 7.497),
    "biel": (47.137, 7.247), "biel/bienne": (47.137, 7.247), "bienne": (47.137, 7.247),
    "thun": (46.758, 7.629), "interlaken": (46.686, 7.863),
    "solothurn": (47.209, 7.538), "olten": (47.352, 7.903), "grenchen": (47.193, 7.396),
    "aarau": (47.391, 8.044), "baden": (47.473, 8.306), "brugg": (47.484, 8.209),
    "wettingen": (47.467, 8.319), "rheinfelden": (47.559, 7.795),
    "liestal": (47.485, 7.736), "pratteln": (47.517, 7.693),
    "binningen": (47.536, 7.568), "reinach": (47.497, 7.590),
    "basel": (47.560, 7.589), "münchenbuchsee": (47.022, 7.456),
    "luzern": (47.050, 8.309), "lucerne": (47.050, 8.309),
    "kriens": (47.032, 8.281), "emmen": (47.075, 8.292),
    "zürich": (47.377, 8.542), "zurich": (47.377, 8.542),
    "winterthur": (47.501, 8.724), "uster": (47.349, 8.720),
    "dübendorf": (47.397, 8.618), "kloten": (47.450, 8.584),
    "dietikon": (47.403, 8.401), "horgen": (47.258, 8.597),
    "zug": (47.166, 8.515), "baar": (47.196, 8.527),
    "fribourg": (46.807, 7.162), "freiburg": (46.807, 7.162),
    "neuchâtel": (47.000, 6.933), "neuenburg": (47.000, 6.933),
    "delémont": (47.366, 7.344),
    "lausanne": (46.520, 6.632), "genève": (46.204, 6.143),
    "geneva": (46.204, 6.143), "genf": (46.204, 6.143),
    "sion": (46.232, 7.360), "sitten": (46.232, 7.360),
    "lugano": (46.004, 8.951), "bellinzona": (46.196, 9.024),
    "st. gallen": (47.424, 9.377), "schaffhausen": (47.696, 8.634),
    "frauenfeld": (47.558, 8.897), "chur": (46.850, 9.533),
    "schwyz": (47.021, 8.651), "altdorf": (46.881, 8.645),
    "stans": (46.958, 8.366), "sarnen": (46.897, 8.247),
    "glarus": (47.040, 9.068), "herisau": (47.388, 9.275),
    "appenzell": (47.331, 9.410),
}


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between two (lat, lon) points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def _resolve_coords(
    canton: str | None,
    municipality: str | None,
    lat: float | None,
    lon: float | None,
) -> tuple[float, float] | None:
    if lat is not None and lon is not None:
        return (lat, lon)
    coords: tuple[float, float] | None = None
    if municipality:
        coords = _MUNICIPALITY_COORDS.get(municipality.lower())
    if coords is None and canton:
        coords = _CANTON_COORDS.get(canton.upper())
    return coords


def _distance_score(
    origin_lat: float,
    origin_lon: float,
    canton: str | None,
    municipality: str | None,
    lat: float | None,
    lon: float | None,
    config: "dict[str, str] | None",
) -> int:
    """Return distance-based score using configurable tier thresholds."""
    coords = _resolve_coords(canton, municipality, lat, lon)
    if coords is None:
        return 0
    dist = _haversine_km(origin_lat, origin_lon, coords[0], coords[1])
    if dist <= 15:
        return _cfg_int(config, "scoring_dist_15km", 20)
    elif dist <= 40:
        return _cfg_int(config, "scoring_dist_40km", 10)
    elif dist <= 80:
        return _cfg_int(config, "scoring_dist_80km", 5)
    elif dist <= 130:
        return _cfg_int(config, "scoring_dist_130km", 0)
    else:
        return _cfg_int(config, "scoring_dist_far", -5)


def distance_to_muri_km(
    *,
    canton: str | None,
    municipality: str | None,
    lat: float | None = None,
    lon: float | None = None,
) -> float | None:
    """Return distance in km to Muri bei Bern (used for batch ordering)."""
    coords = _resolve_coords(canton, municipality, lat, lon)
    return _haversine_km(_ORIGIN[0], _ORIGIN[1], coords[0], coords[1]) if coords else None


def distance_to_origin_km(
    origin_lat: float,
    origin_lon: float,
    *,
    canton: str | None,
    municipality: str | None,
    lat: float | None = None,
    lon: float | None = None,
) -> float | None:
    """Return distance in km from a configurable origin to the company's resolved coordinates."""
    coords = _resolve_coords(canton, municipality, lat, lon)
    return _haversine_km(origin_lat, origin_lon, coords[0], coords[1]) if coords else None


# ── Individual scoring ───────────────────────────────────────────────────────

_DEFAULT_SCORING_CONFIG: dict[str, str] = {
    # Comma-separated cluster label substrings — each match adds cluster_hit_points
    "scoring_target_clusters": "",
    "scoring_cluster_hit_points": "10",
    # Comma-separated cluster label substrings — each match subtracts cluster_exclude_points
    "scoring_exclude_clusters": "",
    "scoring_cluster_exclude_points": "10",
    # Comma-separated purpose keyword substrings — each match adds keyword_hit_points
    "scoring_target_keywords": "",
    "scoring_keyword_hit_points": "10",
    # Comma-separated purpose keyword substrings — each match subtracts keyword_exclude_points
    "scoring_exclude_keywords": "",
    "scoring_keyword_exclude_points": "10",
    # Distance tiers (haversine from configurable origin)
    "scoring_origin_lat": "46.9266",   # default: Muri bei Bern
    "scoring_origin_lon": "7.4817",
    "scoring_dist_15km":  "20",        # pts for ≤ 15 km
    "scoring_dist_40km":  "10",        # pts for ≤ 40 km
    "scoring_dist_80km":  "5",         # pts for ≤ 80 km
    "scoring_dist_130km": "0",         # pts for ≤ 130 km
    "scoring_dist_far":   "-5",        # pts for > 130 km
    # Legal form: "short_name:points" pairs, comma-separated (case-insensitive)
    "scoring_legal_form_scores": "gmbh:20,sarl:20,sàrl:20,einzelfirma:15,eg:15,kg:10,og:8,ag:8,sa:8,stiftung:3,verein:2",
    "scoring_legal_form_default": "5",
    # Fixed score for cancelled/dissolved companies (bypasses normalization)
    "scoring_cancelled_score": "5",
    # Data quality penalties
    "scoring_no_keywords_penalty": "10",       # deducted when purpose_keywords is empty
    "scoring_undefined_cluster_penalty": "10", # deducted when tfidf_cluster is undefined/missing
    # Claude input token optimisation
    "scoring_claude_max_purpose_chars": "800", # purpose text truncated to this many chars before sending to Claude
}

_CANCELLED_STATUS_TERMS = frozenset({"being_cancelled", "dissolved", "gelöscht", "radiation", "liquidation"})


def get_default_scoring_config() -> dict[str, str]:
    return dict(_DEFAULT_SCORING_CONFIG)


def _cfg_int(config: dict[str, str] | None, key: str, fallback: int) -> int:
    if not config:
        return fallback
    raw = config.get(key)
    if raw is None:
        return fallback
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return fallback


def _cfg_float(config: dict[str, str] | None, key: str, fallback: float) -> float:
    if not config:
        return fallback
    raw = config.get(key)
    if raw is None:
        return fallback
    try:
        return float(str(raw).strip())
    except (TypeError, ValueError):
        return fallback


def _cfg_terms(config: dict[str, str] | None, key: str, fallback: list[str]) -> list[str]:
    if not config:
        return fallback
    raw = (config.get(key) or "").strip()
    if not raw:
        return fallback
    return [part.strip().lower() for part in raw.split(",") if part.strip()]


def _parse_legal_form_scores(config: dict[str, str] | None) -> dict[str, int]:
    raw = (config or {}).get("scoring_legal_form_scores") or _DEFAULT_SCORING_CONFIG["scoring_legal_form_scores"]
    result: dict[str, int] = {}
    for part in raw.split(","):
        part = part.strip()
        if ":" in part:
            key, _, val = part.partition(":")
            try:
                result[key.strip().lower()] = int(val.strip())
            except ValueError:
                pass
    return result


def _is_cancelled(status: str | None) -> bool:
    norm = (status or "").lower().replace("-", "_").replace(" ", "_")
    return any(t in norm for t in _CANCELLED_STATUS_TERMS)


def compute_zefix_score_breakdown(
    *,
    legal_form: str | None,
    legal_form_short_name: str | None,
    status: str | None,
    canton: str | None = None,
    municipality: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    purpose_keywords: str | None = None,
    tfidf_cluster: str | None = None,
    config: dict[str, str] | None = None,
    # Legacy params accepted but ignored (kept for backward-compat with old call sites)
    capital_nominal: str | None = None,
    purpose: str | None = None,
    branch_offices: str | None = None,
) -> dict:
    cancelled_score = _cfg_int(config, "scoring_cancelled_score", 5)

    breakdown: dict = {
        "clusters": 0,
        "keywords": 0,
        "distance": 0,
        "legal_form": 0,
        "data_quality": 0,
        "raw_total": 0,
        "final_score": 0,
        "cancelled": False,
    }

    if _is_cancelled(status):
        breakdown["cancelled"] = True
        breakdown["final_score"] = cancelled_score
        return breakdown

    # ── Cluster hits ──────────────────────────────────────────────────────────
    target_clusters = _cfg_terms(config, "scoring_target_clusters", [])
    cluster_pts = _cfg_int(config, "scoring_cluster_hit_points", 10)
    exclude_clusters = _cfg_terms(config, "scoring_exclude_clusters", [])
    cluster_excl_pts = _cfg_int(config, "scoring_cluster_exclude_points", 10)
    if tfidf_cluster:
        cluster_lower = tfidf_cluster.lower()
        if target_clusters:
            hits = sum(1 for tc in target_clusters if tc in cluster_lower)
            breakdown["clusters"] += hits * cluster_pts
        if exclude_clusters:
            excl_hits = sum(1 for ec in exclude_clusters if ec in cluster_lower)
            breakdown["clusters"] -= excl_hits * cluster_excl_pts

    # ── Keyword hits / penalties ───────────────────────────────────────────────
    target_keywords = _cfg_terms(config, "scoring_target_keywords", [])
    kw_pts = _cfg_int(config, "scoring_keyword_hit_points", 10)
    exclude_keywords = _cfg_terms(config, "scoring_exclude_keywords", [])
    kw_excl_pts = _cfg_int(config, "scoring_keyword_exclude_points", 10)
    if purpose_keywords:
        kw_lower = purpose_keywords.lower()
        if target_keywords:
            hits = sum(1 for kw in target_keywords if kw in kw_lower)
            breakdown["keywords"] += hits * kw_pts
        if exclude_keywords:
            excl_hits = sum(1 for ek in exclude_keywords if ek in kw_lower)
            breakdown["keywords"] -= excl_hits * kw_excl_pts

    # ── Data quality penalties ────────────────────────────────────────────────
    no_kw_penalty = _cfg_int(config, "scoring_no_keywords_penalty", 10)
    undef_cluster_penalty = _cfg_int(config, "scoring_undefined_cluster_penalty", 10)
    if not purpose_keywords or not purpose_keywords.strip():
        breakdown["data_quality"] -= no_kw_penalty
    _undef_cluster_terms = {"undefined", "unbekannt", "unknown", "none", "other", "sonstige"}
    if not tfidf_cluster or tfidf_cluster.lower().strip() in _undef_cluster_terms:
        breakdown["data_quality"] -= undef_cluster_penalty

    # ── Distance ──────────────────────────────────────────────────────────────
    origin_lat = _cfg_float(config, "scoring_origin_lat", _ORIGIN[0])
    origin_lon = _cfg_float(config, "scoring_origin_lon", _ORIGIN[1])
    breakdown["distance"] = _distance_score(origin_lat, origin_lon, canton, municipality, lat, lon, config)

    # ── Legal form ────────────────────────────────────────────────────────────
    lf_scores = _parse_legal_form_scores(config)
    lf_default = _cfg_int(config, "scoring_legal_form_default", 5)
    lf_key = (legal_form_short_name or legal_form or "").lower().strip()
    breakdown["legal_form"] = lf_scores.get(lf_key, lf_default) if lf_key else lf_default

    raw = (
        int(breakdown["clusters"])
        + int(breakdown["keywords"])
        + int(breakdown["distance"])
        + int(breakdown["legal_form"])
        + int(breakdown["data_quality"])
    )
    breakdown["raw_total"] = raw
    # Clamped to 0-100 for real-time use; recalculate job normalises properly
    breakdown["final_score"] = max(0, min(100, raw))
    return breakdown


def compute_zefix_score(
    *,
    legal_form: str | None,
    legal_form_short_name: str | None,
    status: str | None,
    canton: str | None = None,
    municipality: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    purpose_keywords: str | None = None,
    tfidf_cluster: str | None = None,
    config: dict[str, str] | None = None,
    # Legacy compat
    capital_nominal: str | None = None,
    purpose: str | None = None,
    branch_offices: str | None = None,
) -> int:
    return int(compute_zefix_score_breakdown(
        legal_form=legal_form,
        legal_form_short_name=legal_form_short_name,
        status=status,
        canton=canton,
        municipality=municipality,
        lat=lat,
        lon=lon,
        purpose_keywords=purpose_keywords,
        tfidf_cluster=tfidf_cluster,
        config=config,
    )["final_score"])


def normalize_raw_scores(
    raw_scores: dict[int, int | None],
    cancelled_score: int = 5,
) -> dict[int, int]:
    """Min-max normalize raw scores to 0-100. Cancelled (None) → cancelled_score."""
    non_cancelled = {cid: s for cid, s in raw_scores.items() if s is not None}
    result: dict[int, int] = {}
    if non_cancelled:
        min_s = min(non_cancelled.values())
        max_s = max(non_cancelled.values())
        for cid, raw in non_cancelled.items():
            result[cid] = round((raw - min_s) / (max_s - min_s) * 100) if max_s > min_s else 50
    for cid, raw in raw_scores.items():
        if raw is None:
            result[cid] = cancelled_score
    return result
