from datetime import datetime

from pydantic import BaseModel, ConfigDict, field_validator

from app.schemas.note import NoteRead


def _coerce_multilang(v: object) -> str | None:
    """Extract a string from a Zefix multilingual dict or pass through as-is."""
    if isinstance(v, dict):
        return (
            v.get("de") or v.get("fr") or v.get("it") or v.get("en")
            or v.get("shortName") or next(iter(v.values()), None) or None
        )
    return v or None  # type: ignore[return-value]


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
    # Zefix administrative identifiers
    ehraid: str | None = None
    chid: str | None = None
    legal_seat_id: int | None = None
    legal_form_id: int | None = None
    legal_form_uid: str | None = None
    legal_form_short_name: str | None = None
    sogc_date: str | None = None
    deletion_date: str | None = None
    # Extended detail fields (from per-UID Zefix endpoint)
    sogc_pub: str | None = None
    capital_nominal: str | None = None
    capital_currency: str | None = None
    head_offices: str | None = None
    further_head_offices: str | None = None
    branch_offices: str | None = None
    has_taken_over: str | None = None
    was_taken_over_by: str | None = None
    audit_companies: str | None = None
    old_names: str | None = None
    cantonal_excerpt_web: str | None = None
    zefix_score: int | None = None
    zefix_score_breakdown: str | None = None
    lat: float | None = None
    lon: float | None = None
    tfidf_cluster: str | None = None
    claude_score: int | None = None
    claude_category: str | None = None
    claude_freeform: str | None = None

    @field_validator("legal_form", "status", "municipality", "canton", "purpose", mode="before")
    @classmethod
    def coerce_multilang_fields(cls, v: object) -> str | None:
        return _coerce_multilang(v)


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
    social_media_only: bool | None = None
    review_status: str | None = None
    proposal_status: str | None = None
    contact_name: str | None = None
    contact_email: str | None = None
    contact_phone: str | None = None
    tags: str | None = None
    # Zefix administrative identifiers
    ehraid: str | None = None
    chid: str | None = None
    legal_seat_id: int | None = None
    legal_form_id: int | None = None
    legal_form_uid: str | None = None
    legal_form_short_name: str | None = None
    sogc_date: str | None = None
    deletion_date: str | None = None
    # Extended detail fields
    sogc_pub: str | None = None
    capital_nominal: str | None = None
    capital_currency: str | None = None
    head_offices: str | None = None
    further_head_offices: str | None = None
    branch_offices: str | None = None
    has_taken_over: str | None = None
    was_taken_over_by: str | None = None
    audit_companies: str | None = None
    old_names: str | None = None
    cantonal_excerpt_web: str | None = None
    zefix_score: int | None = None
    zefix_score_breakdown: str | None = None
    lat: float | None = None
    lon: float | None = None
    tfidf_cluster: str | None = None
    claude_score: int | None = None
    claude_category: str | None = None
    claude_freeform: str | None = None
    claude_scored_at: datetime | None = None
    zefix_scored_at: datetime | None = None


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
    created_at: datetime
    updated_at: datetime
    notes: list[NoteRead] = []


class ZefixSearchResult(BaseModel):
    """Lightweight result returned directly from the Zefix API search."""

    uid: str
    name: str
    legal_form: str | None = None
    status: str | None = None
    municipality: str | None = None
    canton: str | None = None
    purpose: str | None = None
    # Extended fields populated from search response when available
    ehraid: str | None = None
    chid: str | None = None
    legal_seat_id: int | None = None
    legal_form_id: int | None = None
    legal_form_uid: str | None = None
    legal_form_short_name: str | None = None
    sogc_date: str | None = None
    deletion_date: str | None = None

    @field_validator("legal_form", "status", "municipality", "canton", "purpose", mode="before")
    @classmethod
    def coerce_multilang_fields(cls, v: object) -> str | None:
        return _coerce_multilang(v)


class GoogleSearchResult(BaseModel):
    """Result from a Google Custom Search query."""

    title: str
    link: str
    snippet: str | None = None
