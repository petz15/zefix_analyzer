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

# Digits + letters for prefix sweep (companies can start with a number)
ALPHANUMERIC = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

# Hard cap accepted by the Zefix search endpoint
ZEFIX_MAX_ENTRIES = 20000


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
    """Fetch one page of companies for a given canton."""
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
    """Fetch all companies whose name starts with *prefix*, optionally filtered by canton."""
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

    data = response.json()
    if isinstance(data, list):
        if not data:
            raise ValueError(f"No company found for UID {uid}")
        data = data[0]
    return data


def _parse_legal_form(lf: Any) -> tuple[str | None, int | None, str | None, str | None]:
    """Parse a legalForm value into (display_name, id, uid, short_name).

    Handles both the flat dict ``{"de": "...", "shortName": "AG"}`` returned by
    the search endpoint and the nested dict returned by the detail endpoint:
    ``{"id": 1, "uid": "...", "name": {"de": "..."}, "shortName": {"de": "AG"}}``.
    """
    if not lf or not isinstance(lf, dict):
        return (str(lf) if lf else None, None, None, None)

    lf_id: int | None = lf.get("id")
    lf_uid: str | None = lf.get("uid") or None

    # Display name: try nested name dict first, then flat de/fr/it/en keys
    name_raw = lf.get("name") or lf
    if isinstance(name_raw, dict):
        display = (
            name_raw.get("de") or name_raw.get("fr") or name_raw.get("it")
            or name_raw.get("en") or None
        )
    else:
        display = str(name_raw) if name_raw else None

    # Fall back to shortName if no display name found
    short_raw = lf.get("shortName") or lf.get("shortNameDe")
    if isinstance(short_raw, dict):
        short = short_raw.get("de") or next(iter(short_raw.values()), None)
    else:
        short = str(short_raw) if short_raw else None

    if not display:
        display = short

    return display, lf_id, lf_uid, short


def _parse_company(data: dict[str, Any]) -> ZefixSearchResult:
    """Map a raw Zefix API company dict to a :class:`ZefixSearchResult`."""
    uid = data.get("uid", "") or ""
    uid = _normalise_uid(uid)

    name_raw = data.get("name", "")
    if isinstance(name_raw, dict):
        name = name_raw.get("de") or name_raw.get("fr") or name_raw.get("it") or next(iter(name_raw.values()), "")
    else:
        name = str(name_raw)

    legal_form_display, legal_form_id, legal_form_uid, legal_form_short = _parse_legal_form(
        data.get("legalForm")
    )

    status_raw = data.get("status", None)
    if isinstance(status_raw, dict):
        status = (
            status_raw.get("de") or status_raw.get("en")
            or next(iter(status_raw.values()), None) or None
        )
    else:
        status = str(status_raw) if status_raw else None

    municipality = data.get("municipality") or data.get("legalSeat") or None
    canton = data.get("canton") or None

    purpose_raw = data.get("purpose") or data.get("purposes") or None
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

    # Administrative identifiers
    ehraid_raw = data.get("ehraId") or data.get("ehraid") or data.get("ehra_id")
    ehraid = str(ehraid_raw) if ehraid_raw is not None else None

    chid_raw = data.get("chid")
    chid = str(chid_raw) if chid_raw is not None else None

    legal_seat_id_raw = data.get("legalSeatId") or data.get("legal_seat_id")
    legal_seat_id: int | None = None
    if legal_seat_id_raw is not None:
        try:
            legal_seat_id = int(legal_seat_id_raw)
        except (ValueError, TypeError):
            pass

    sogc_date_raw = data.get("sogcDate") or data.get("sogc_date")
    sogc_date = str(sogc_date_raw) if sogc_date_raw else None

    deletion_date_raw = data.get("deletionDate") or data.get("deletion_date")
    deletion_date = str(deletion_date_raw) if deletion_date_raw else None

    return ZefixSearchResult(
        uid=uid,
        name=name,
        legal_form=legal_form_display,
        legal_form_id=legal_form_id,
        legal_form_uid=legal_form_uid,
        legal_form_short_name=legal_form_short,
        status=status,
        municipality=municipality,
        canton=canton,
        purpose=purpose,
        ehraid=ehraid,
        chid=chid,
        legal_seat_id=legal_seat_id,
        sogc_date=sogc_date,
        deletion_date=deletion_date,
    )


def _normalise_uid(uid: str) -> str:
    """Return the UID in ``CHE-XXX.XXX.XXX`` format when possible."""
    digits = "".join(ch for ch in uid if ch.isdigit())
    if len(digits) == 9:
        return f"CHE-{digits[:3]}.{digits[3:6]}.{digits[6:9]}"
    return uid
