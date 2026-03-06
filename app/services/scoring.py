"""Scoring logic for matching Google Search results to a company profile."""

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
