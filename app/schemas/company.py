from datetime import datetime

from pydantic import BaseModel, ConfigDict

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


class ZefixSearchResult(BaseModel):
    """Lightweight result returned directly from the Zefix API search."""

    uid: str
    name: str
    legal_form: str | None = None
    status: str | None = None
    municipality: str | None = None
    canton: str | None = None


class GoogleSearchResult(BaseModel):
    """Result from a Google Custom Search query."""

    title: str
    link: str
    snippet: str | None = None
