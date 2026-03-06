"""Client for the Google Custom Search JSON API.

Documentation: https://developers.google.com/custom-search/v1/using_rest
"""

import httpx

from app.config import settings
from app.schemas.company import GoogleSearchResult

_GOOGLE_SEARCH_URL = "https://www.googleapis.com/customsearch/v1"


def search_website(company_name: str, *, num: int = 5) -> list[GoogleSearchResult]:
    """Search for a company's website using Google Custom Search.

    Args:
        company_name: The company name to search for.
        num: Number of results to return (1-10, as limited by the API).

    Returns:
        A list of :class:`GoogleSearchResult` instances.

    Raises:
        ValueError: If the Google API key or CSE ID is not configured.
        httpx.HTTPStatusError: If the Google API returns a non-2xx response.
    """
    if not settings.google_api_key or not settings.google_cse_id:
        raise ValueError(
            "GOOGLE_API_KEY and GOOGLE_CSE_ID must be set to use the Google Search integration."
        )

    params = {
        "key": settings.google_api_key,
        "cx": settings.google_cse_id,
        "q": company_name,
        "num": min(max(1, num), 10),
    }

    with httpx.Client(timeout=30.0) as client:
        response = client.get(_GOOGLE_SEARCH_URL, params=params)
        response.raise_for_status()

    data = response.json()
    items = data.get("items", [])
    return [
        GoogleSearchResult(
            title=item.get("title", ""),
            link=item.get("link", ""),
            snippet=item.get("snippet"),
        )
        for item in items
    ]
