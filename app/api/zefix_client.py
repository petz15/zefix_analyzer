"""Client for the Zefix REST API (https://www.zefix.admin.ch/ZefixREST/api/v1)."""

import json
from typing import Any

import httpx

from app.config import settings
from app.schemas.company import ZefixSearchResult


def _get_auth() -> httpx.BasicAuth | None:
    if settings.zefix_api_username and settings.zefix_api_password:
        return httpx.BasicAuth(settings.zefix_api_username, settings.zefix_api_password)
    return None


def search_companies(
    name: str,
    *,
    max_results: int = 20,
    active_only: bool = False,
) -> list[ZefixSearchResult]:
    """Search for companies by name via the Zefix API.

    Args:
        name: Company name (or partial name) to search for.
        max_results: Maximum number of results to return (server-side cap may apply).
        active_only: If True, only return companies with an active entry.

    Returns:
        A list of :class:`ZefixSearchResult` instances.

    Raises:
        httpx.HTTPStatusError: If the Zefix API returns a non-2xx response.
    """
    url = f"{settings.zefix_api_base_url}/company/search"
    payload: dict[str, Any] = {"name": name, "maxEntries": max_results, "languageKey": "en"}
    if active_only:
        payload["activeOnly"] = True

    with httpx.Client(timeout=30.0) as client:
        response = client.post(url, json=payload, auth=_get_auth())
        response.raise_for_status()

    data = response.json()
    # The API returns a list of company objects directly
    items = data if isinstance(data, list) else data.get("list", [])
    return [_parse_company(item) for item in items]


def get_company(uid: str) -> dict[str, Any]:
    """Fetch full company details by UID from the Zefix API.

    Args:
        uid: The Swiss UID (e.g. ``CHE-123.456.789``).

    Returns:
        The raw response JSON as a dict.

    Raises:
        httpx.HTTPStatusError: If the Zefix API returns a non-2xx response.
    """
    # Normalise UID: the API expects no dashes for the path parameter
    uid_clean = uid.replace("-", "").replace(".", "")
    url = f"{settings.zefix_api_base_url}/company/uid/{uid_clean}"

    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, auth=_get_auth())
        response.raise_for_status()

    return response.json()


def _parse_company(data: dict[str, Any]) -> ZefixSearchResult:
    """Map a raw Zefix API company dict to a :class:`ZefixSearchResult`."""
    uid = data.get("uid", "") or ""
    # Zefix returns UID as numeric string; normalise to ``CHE-XXX.XXX.XXX``
    uid = _normalise_uid(uid)

    # Name may be a dict keyed by language
    name_raw = data.get("name", "")
    if isinstance(name_raw, dict):
        name = name_raw.get("de") or name_raw.get("fr") or name_raw.get("it") or next(iter(name_raw.values()), "")
    else:
        name = str(name_raw)

    legal_form_raw = data.get("legalForm", {})
    if isinstance(legal_form_raw, dict):
        legal_form = legal_form_raw.get("de") or legal_form_raw.get("shortName") or None
    else:
        legal_form = str(legal_form_raw) if legal_form_raw else None

    status_raw = data.get("status", None)
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
