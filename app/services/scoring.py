"""Scoring logic for matching Google Search results to a company profile."""

import math
import re
from urllib.parse import urlparse

# Domains that are business directories, social networks, or government registries.
_DIRECTORY_DOMAINS = {
    "linkedin.com",
    "facebook.com",
    "twitter.com",
    "x.com",
    "xing.com",
    "instagram.com",
    "youtube.com",
    "wikipedia.org",
    "zefix.admin.ch",
    "uid.admin.ch",
    "moneyhouse.ch",
    "shab.ch",
    "search.ch",
    "local.ch",
    "yellowpages.ch",
    "directories.ch",
    "scout24.ch",
    "homegate.ch",
    "companyhouse.ch",
    "handelsregister.ch",
    "hr-register.ch",
}

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


def score_result(
    result: dict,
    *,
    company_name: str,
    municipality: str | None,
    canton: str | None,
    purpose: str | None = None,
    legal_form: str | None = None,
) -> int:
    """Score a single Google search result against a company profile.

    Returns an integer 0-100.

    Breakdown:
      - Name match in title:        0-40 pts  (word overlap × 40)
      - Name match in snippet:      0-10 pts  (bonus if overlap > 0.5)
      - Location in combined text:  0-30 pts  (municipality 20 + canton 10)
      - Purpose keyword match:      0-10 pts  (1+ hit = 5, 3+ hits = 10)
      - Legal form in domain/title:   +5 pts  bonus
      - Directory domain:            -20 pts  penalty
    """
    title = result.get("title", "") or ""
    snippet = result.get("snippet", "") or ""
    link = result.get("link", "") or ""
    combined = f"{title} {snippet}"
    combined_lower = combined.lower()

    # --- Name in title (0-40) ---
    score = int(_word_overlap_ratio(company_name, title) * 40)

    # --- Name in snippet bonus (0-10) ---
    if _word_overlap_ratio(company_name, snippet) > 0.5:
        score += 10

    # --- Location match (0-30) ---
    if municipality and municipality.lower() in combined_lower:
        score += 20
    if canton and canton.upper() in combined.upper():
        score += 10

    # --- Purpose keyword match (0-10) ---
    keywords = _purpose_keywords(purpose)
    if keywords:
        hits = sum(1 for kw in keywords if kw in combined_lower)
        if hits >= 3:
            score += 10
        elif hits >= 1:
            score += 5

    # --- Legal form presence in domain or title (+5 bonus) ---
    if legal_form:
        domain = _root_domain(link)
        lf_lower = legal_form.lower()
        # Common abbreviations: ag, gmbh, sa, sarl, kg, oG, etc.
        abbrevs = re.findall(r"\b\w{2,6}\b", lf_lower)
        if any(a in domain or a in title.lower() for a in abbrevs if len(a) >= 2):
            score += 5

    # --- Directory domain penalty ---
    domain = _root_domain(link)
    if any(domain == d or domain.endswith("." + d) for d in _DIRECTORY_DOMAINS):
        score -= 20

    return max(0, min(100, score))


# ── Location scoring ────────────────────────────────────────────────────────
# Origin: Muri bei Bern (lat, lon)
_ORIGIN = (46.9266, 7.4817)

# Cantons within reasonable business-travel distance get a bonus; all others a deduction.
# User-specified nearby cantons: BE, LU, BL, SO, BS, AG, ZH
_CANTON_LOCATION_SCORE: dict[str, int] = {
    "BE": 10, "SO": 10,          # immediate neighbours
    "AG": 8,                      # ~65 km
    "BL": 6, "BS": 6,             # ~70 km
    "LU": 5,                      # ~75 km
    "ZH": 4,                      # ~115 km
}
_CANTON_LOCATION_DEFAULT = -8   # all other cantons

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


def _location_score(
    canton: str | None,
    municipality: str | None,
    lat: float | None = None,
    lon: float | None = None,
) -> int:
    """Return a location score (may be negative) based on proximity to Muri bei Bern.

    Two components:
      1. Canton tier  — fixed bonus/deduction per the nearby-canton list.
      2. Distance tier — ±pts based on Haversine km from the origin, resolved as:
           a) exact geocoded (lat, lon) when provided,
           b) municipality name lookup,
           c) canton centroid fallback.
    """
    canton_score = _CANTON_LOCATION_SCORE.get((canton or "").upper(), _CANTON_LOCATION_DEFAULT)

    # Resolve company coordinates — best available source wins
    if lat is not None and lon is not None:
        company_coords: tuple[float, float] | None = (lat, lon)
    else:
        company_coords = None
        if municipality:
            company_coords = _MUNICIPALITY_COORDS.get(municipality.lower())
        if company_coords is None and canton:
            company_coords = _CANTON_COORDS.get(canton.upper())

    if company_coords is None:
        return canton_score

    dist = _haversine_km(_ORIGIN[0], _ORIGIN[1], company_coords[0], company_coords[1])
    if dist <= 15:
        distance_score = 15
    elif dist <= 40:
        distance_score = 10
    elif dist <= 80:
        distance_score = 5
    elif dist <= 130:
        distance_score = 0
    else:
        distance_score = -5

    return canton_score + distance_score


# ── Legal form scoring ───────────────────────────────────────────────────────
# Legal forms that correlate with operating businesses worth reaching out to.
# Higher score = more likely to be a real, reachable company.
_LEGAL_FORM_SCORES: dict[str, int] = {
    "ag": 10, "sa": 10,          # Aktiengesellschaft / Société anonyme
    "gmbh": 25, "sàrl": 25, "sarl": 25,  # GmbH variants
    "eg": 20, "genossenschaft": 20,       # Cooperatives
    "kg": 15, "kommanditgesellschaft": 15,
    "og": 12, "kollektivgesellschaft": 12,
    "EIU": 30, "eiu": 30, "einzelfirma": 30,  # Sole proprietorships
    "stiftung": 8, "fondation": 8,        # Foundations — registered but may not be commercial
    "verein": 5, "association": 5,        # Associations — usually non-commercial
}

_DEFAULT_SCORING_CONFIG: dict[str, str] = {
    "zefix_industry_bonus": "15",
    "zefix_treuhand_consulting_penalty": "15",
    "zefix_inactive_status_penalty": "40",
    "zefix_force_zero_status_terms": "being_cancelled",
}


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


def _cfg_terms(config: dict[str, str] | None, key: str, fallback: list[str]) -> list[str]:
    if not config:
        return fallback
    raw = (config.get(key) or "").strip()
    if not raw:
        return fallback
    return [part.strip().lower().replace("-", "_").replace(" ", "_") for part in raw.split(",") if part.strip()]


def compute_zefix_score_breakdown(
    *,
    legal_form: str | None,
    legal_form_short_name: str | None,
    capital_nominal: str | None,
    purpose: str | None,
    branch_offices: str | None,
    industry: str | None,
    status: str | None,
    canton: str | None = None,
    municipality: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    config: dict[str, str] | None = None,
) -> dict:
    industry_bonus = _cfg_int(config, "zefix_industry_bonus", 15)
    consulting_penalty = _cfg_int(config, "zefix_treuhand_consulting_penalty", 15)
    inactive_penalty = _cfg_int(config, "zefix_inactive_status_penalty", 40)
    force_zero_terms = _cfg_terms(config, "zefix_force_zero_status_terms", ["being_cancelled"])

    breakdown: dict[str, int | str | list[str] | bool] = {
        "legal_form": 0,
        "capital": 0,
        "purpose": 0,
        "branch_offices": 0,
        "industry_bonus": 0,
        "industry_penalty": 0,
        "location": 0,
        "status_penalty": 0,
        "forced_zero": False,
        "forced_zero_reason": "",
        "force_zero_terms": force_zero_terms,
    }

    status_norm = (status or "").lower().replace("-", "_").replace(" ", "_")
    if status_norm and any(term and term in status_norm for term in force_zero_terms):
        breakdown["forced_zero"] = True
        breakdown["forced_zero_reason"] = status or ""
        breakdown["final_score"] = 0
        return breakdown

    lf_key = (legal_form_short_name or legal_form or "").lower().strip()
    breakdown["legal_form"] = _LEGAL_FORM_SCORES.get(lf_key, 5)

    if capital_nominal:
        try:
            cap = float(str(capital_nominal).replace("'", "").replace(",", "").replace(" ", ""))
            if cap > 100_000:
                breakdown["capital"] = 10
            elif cap > 0:
                breakdown["capital"] = 5
        except (ValueError, TypeError):
            breakdown["capital"] = 5

    if purpose:
        words = len(re.findall(r"\w+", purpose))
        if words >= 20:
            breakdown["purpose"] = 20
        elif words >= 8:
            breakdown["purpose"] = 5

    if branch_offices and branch_offices not in ("null", "[]", ""):
        breakdown["branch_offices"] = 10

    if industry:
        breakdown["industry_bonus"] = industry_bonus
        ind = industry.lower()
        if "treuhand" in ind or "consulting" in ind:
            breakdown["industry_penalty"] = -consulting_penalty

    breakdown["location"] = _location_score(canton, municipality, lat=lat, lon=lon)

    if status:
        st = status.lower()
        if not any(word in st for word in ("aktiv", "active", "eingetragen", "inscrit", "iscritto")):
            breakdown["status_penalty"] = -inactive_penalty

    total = (
        int(breakdown["legal_form"])
        + int(breakdown["capital"])
        + int(breakdown["purpose"])
        + int(breakdown["branch_offices"])
        + int(breakdown["industry_bonus"])
        + int(breakdown["industry_penalty"])
        + int(breakdown["location"])
        + int(breakdown["status_penalty"])
    )
    breakdown["final_score"] = max(0, min(100, total))
    return breakdown


def compute_zefix_score(
    *,
    legal_form: str | None,
    legal_form_short_name: str | None,
    capital_nominal: str | None,
    purpose: str | None,
    branch_offices: str | None,
    industry: str | None,
    status: str | None,
    canton: str | None = None,
    municipality: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    config: dict[str, str] | None = None,
) -> int:
    breakdown = compute_zefix_score_breakdown(
        legal_form=legal_form,
        legal_form_short_name=legal_form_short_name,
        capital_nominal=capital_nominal,
        purpose=purpose,
        branch_offices=branch_offices,
        industry=industry,
        status=status,
        canton=canton,
        municipality=municipality,
        lat=lat,
        lon=lon,
        config=config,
    )
    return int(breakdown["final_score"])
