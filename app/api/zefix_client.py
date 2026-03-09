"""Client for the Zefix REST API (https://www.zefix.admin.ch/ZefixREST/api/v1)."""

from typing import Any

import httpx

from app.config import settings
from app.schemas.company import ZefixSearchResult

# All 26 Swiss cantons in the order the Zefix API recognises
SWISS_CANTONS = [
    "AG", "AI", "AR", "BE", "BL", "BS", "FR", "GE", "GL", "GR",
    "JU", "LU", "NE", "NW", "OW", "SG", "SH", "SO", "SZ", "TG",
    "TI", "UR", "VD", "VS", "ZG", "ZH",
]

# Letters used for prefix sweep; Zefix name search is case-insensitive
ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

# Hard cap accepted by the Zefix search endpoint
ZEFIX_MAX_ENTRIES = 500


def _get_auth() -> httpx.BasicAuth | None:
    if settings.zefix_api_username and settings.zefix_api_password:
        return httpx.BasicAuth(settings.zefix_api_username, settings.zefix_api_password)
    return None


def search_companies(
    name: str,
    *,
    max_results: int = 20,
    active_only: bool = False,
    canton: str | None = None,
    legal_form: str | None = None,
) -> list[ZefixSearchResult]:
    """Search for companies by name via the Zefix API."""
    url = f"{settings.zefix_api_base_url}/company/search"
    payload: dict[str, Any] = {"name": name, "maxEntries": max_results, "languageKey": "en"}
    if active_only:
        payload["activeOnly"] = True
    if canton:
        payload["canton"] = canton.upper()
    if legal_form:
        payload["legalForm"] = legal_form

    with httpx.Client(timeout=30.0) as client:
        response = client.post(url, json=payload, auth=_get_auth())
        response.raise_for_status()

    data = response.json()
    items = data if isinstance(data, list) else data.get("list", [])
    return [_parse_company(item) for item in items]


def fetch_companies_by_canton(
    canton: str,
    *,
    page_size: int = 200,
    offset: int = 0,
    active_only: bool = True,
) -> list[ZefixSearchResult]:
    """Fetch one page of companies for a given canton.

    Args:
        canton: Two-letter canton code, e.g. ``"ZH"``.
        page_size: Number of results per page (max accepted by the API: 500).
        offset: Zero-based record offset for pagination.
        active_only: When True only include companies with an active register entry.

    Returns:
        List of :class:`ZefixSearchResult`. An empty list signals end-of-pages.
    """
    url = f"{settings.zefix_api_base_url}/company/search"
    payload: dict[str, Any] = {
        "canton": canton,
        "maxEntries": page_size,
        "offset": offset,
        "languageKey": "en",
    }
    if active_only:
        payload["activeOnly"] = True

    with httpx.Client(timeout=30.0) as client:
        response = client.post(url, json=payload, auth=_get_auth())
        response.raise_for_status()

    data = response.json()
    items = data if isinstance(data, list) else data.get("list", [])
    return [_parse_company(item) for item in items]


def fetch_companies_by_prefix(
    prefix: str,
    canton: str | None = None,
    *,
    active_only: bool = True,
) -> list[ZefixSearchResult]:
    """Fetch all companies whose name starts with *prefix*, optionally filtered by canton.

    Uses the maximum page size (ZEFIX_MAX_ENTRIES).  The caller is responsible for
    detecting a full page and expanding to double-letter prefixes if needed.
    """
    url = f"{settings.zefix_api_base_url}/company/search"
    payload: dict[str, Any] = {
        "name": prefix,
        "maxEntries": ZEFIX_MAX_ENTRIES,
        "languageKey": "en",
    }
    if canton:
        payload["canton"] = canton
    if active_only:
        payload["activeOnly"] = True

    with httpx.Client(timeout=30.0) as client:
        response = client.post(url, json=payload, auth=_get_auth())
        response.raise_for_status()

    data = response.json()
    items = data if isinstance(data, list) else data.get("list", [])
    return [_parse_company(item) for item in items]


def get_company(uid: str) -> dict[str, Any]:
    """Fetch full company details by UID from the Zefix API."""
    uid_clean = uid.replace("-", "").replace(".", "")
    url = f"{settings.zefix_api_base_url}/company/uid/{uid_clean}"

    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, auth=_get_auth())
        response.raise_for_status()

    return response.json()


def _parse_company(data: dict[str, Any]) -> ZefixSearchResult:
    """Map a raw Zefix API company dict to a :class:`ZefixSearchResult`."""
    uid = data.get("uid", "") or ""
    uid = _normalise_uid(uid)

    name_raw = data.get("name", "")
    if isinstance(name_raw, dict):
        name = name_raw.get("de") or name_raw.get("fr") or name_raw.get("it") or next(iter(name_raw.values()), "")
    else:
        name = str(name_raw)

    legal_form_raw = data.get("legalForm", {})
    if isinstance(legal_form_raw, dict):
        legal_form = (
            legal_form_raw.get("de") or legal_form_raw.get("fr")
            or legal_form_raw.get("it") or legal_form_raw.get("en")
            or legal_form_raw.get("shortName")
            or next(iter(legal_form_raw.values()), None) or None
        )
    else:
        legal_form = str(legal_form_raw) if legal_form_raw else None

    status_raw = data.get("status", None)
    if isinstance(status_raw, dict):
        status = (
            status_raw.get("de") or status_raw.get("en")
            or next(iter(status_raw.values()), None) or None
        )
    else:
        status = str(status_raw) if status_raw else None

    municipality = data.get("municipality") or None
    canton = data.get("canton") or None

    return ZefixSearchResult(
        uid=uid,
        name=name,
        legal_form=legal_form,
        status=status,
        municipality=municipality,
        canton=canton,
    )


def _normalise_uid(uid: str) -> str:
    """Return the UID in ``CHE-XXX.XXX.XXX`` format when possible."""
    digits = "".join(ch for ch in uid if ch.isdigit())
    if len(digits) == 9:
        return f"CHE-{digits[:3]}.{digits[3:6]}.{digits[6:9]}"
    return uid
