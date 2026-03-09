from datetime import datetime

from pydantic import BaseModel, ConfigDict, field_validator

from app.schemas.note import NoteRead


class CompanyBase(BaseModel):
    uid: str
    name: str
    legal_form: str | None = None
    status: str | None = None
    municipality: str | None = None
    canton: str | None = None
    purpose: str | None = None
    address: str | None = None
    website_url: str | None = None


class CompanyCreate(CompanyBase):
    zefix_raw: str | None = None


class CompanyUpdate(BaseModel):
    name: str | None = None
    legal_form: str | None = None
    status: str | None = None
    municipality: str | None = None
    canton: str | None = None
    purpose: str | None = None
    address: str | None = None
    website_url: str | None = None
    website_checked_at: datetime | None = None
    zefix_raw: str | None = None
    google_search_results_raw: str | None = None
    website_match_score: int | None = None
    review_status: str | None = None
    proposal_status: str | None = None
    contact_name: str | None = None
    contact_email: str | None = None
    contact_phone: str | None = None
    tags: str | None = None
    industry: str | None = None


class CompanyRead(CompanyBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    website_checked_at: datetime | None = None
    google_search_results_raw: str | None = None
    website_match_score: int | None = None
    review_status: str | None = None
    proposal_status: str | None = None
    contact_name: str | None = None
    contact_email: str | None = None
    contact_phone: str | None = None
    tags: str | None = None
    industry: str | None = None
    created_at: datetime
    updated_at: datetime
    notes: list[NoteRead] = []


def _coerce_multilang(v: object) -> str | None:
    """Extract a string from a Zefix multilingual dict or pass through as-is."""
    if isinstance(v, dict):
        return (
            v.get("de") or v.get("fr") or v.get("it") or v.get("en")
            or v.get("shortName") or next(iter(v.values()), None) or None
        )
    return v or None  # type: ignore[return-value]


class ZefixSearchResult(BaseModel):
    """Lightweight result returned directly from the Zefix API search."""

    uid: str
    name: str
    legal_form: str | None = None
    status: str | None = None
    municipality: str | None = None
    canton: str | None = None

    @field_validator("legal_form", "status", "municipality", "canton", mode="before")
    @classmethod
    def coerce_multilang_fields(cls, v: object) -> str | None:
        return _coerce_multilang(v)


class GoogleSearchResult(BaseModel):
    """Result from a Google Custom Search query."""

    title: str
    link: str
    snippet: str | None = None
