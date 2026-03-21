from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class BoilerplatePattern(Base):
    """Regex pattern for stripping Swiss registry boilerplate from purpose texts."""

    __tablename__ = "boilerplate_patterns"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    # The regex pattern (applied with re.IGNORECASE)
    pattern: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    # Human-readable description of what this pattern strips
    description: Mapped[str | None] = mapped_column(String(512), nullable=True)
    # Example sentence this pattern was derived from
    example: Mapped[str | None] = mapped_column(Text, nullable=True)
    # How many sentences this matched during analysis (informational)
    match_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Only active patterns are applied during Claude classification
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
