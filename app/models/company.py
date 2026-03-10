from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Company(Base):
    __tablename__ = "companies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    uid: Mapped[str] = mapped_column(String(20), unique=True, index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    legal_form: Mapped[str | None] = mapped_column(String(128), nullable=True)
    status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    municipality: Mapped[str | None] = mapped_column(String(256), nullable=True)
    canton: Mapped[str | None] = mapped_column(String(8), nullable=True)
    purpose: Mapped[str | None] = mapped_column(Text, nullable=True)
    address: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Website found via Google Search
    website_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    website_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Top-5 Google results as JSON [{title, link, snippet, score}, ...] sorted by score desc
    google_search_results_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    # 0-100 auto match score for the current website_url; None = not yet scored
    website_match_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Manual workflow statuses
    # 'pending' | 'confirmed' | 'interesting' | 'rejected'
    review_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    # 'not_sent' | 'sent' | 'responded' | 'converted' | 'rejected'
    proposal_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    # Contact info (manually entered)
    contact_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    contact_email: Mapped[str | None] = mapped_column(String(512), nullable=True)
    contact_phone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Comma-separated free-form labels, e.g. "saas,b2b,warm-lead"
    tags: Mapped[str | None] = mapped_column(Text, nullable=True)
    industry: Mapped[str | None] = mapped_column(String(128), nullable=True)
    # Zefix administrative identifiers
    ehraid: Mapped[str | None] = mapped_column(String(64), nullable=True)
    chid: Mapped[str | None] = mapped_column(String(32), nullable=True)
    legal_seat_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    legal_form_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    legal_form_uid: Mapped[str | None] = mapped_column(String(64), nullable=True)
    legal_form_short_name: Mapped[str | None] = mapped_column(String(32), nullable=True)
    sogc_date: Mapped[str | None] = mapped_column(String(32), nullable=True)
    deletion_date: Mapped[str | None] = mapped_column(String(32), nullable=True)
    # Extended Zefix detail fields (populated from per-UID endpoint only)
    sogc_pub: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON array
    capital_nominal: Mapped[str | None] = mapped_column(String(64), nullable=True)
    capital_currency: Mapped[str | None] = mapped_column(String(16), nullable=True)
    head_offices: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON
    further_head_offices: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON
    branch_offices: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON
    has_taken_over: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON
    was_taken_over_by: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON
    audit_companies: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON
    old_names: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON
    cantonal_excerpt_web: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    # Priority/lead score derived from Zefix data alone (0-100)
    zefix_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Geocoded coordinates (from Nominatim, based on the Zefix address)
    lat: Mapped[float | None] = mapped_column(Float, nullable=True)
    lon: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Raw JSON from Zefix API stored for reference
    zefix_raw: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    notes: Mapped[list["Note"]] = relationship("Note", back_populates="company", cascade="all, delete-orphan")
