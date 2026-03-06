from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class CollectionRun(Base):
    """Tracks bulk/batch collection runs; provides resume checkpoints for bulk imports."""

    __tablename__ = "collection_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    # 'bulk' | 'batch'
    run_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Checkpoint: last completed canton + offset (for bulk resume)
    last_canton: Mapped[str | None] = mapped_column(String(8), nullable=True)
    last_offset: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # JSON snapshot of stats at completion (or last checkpoint)
    stats_json: Mapped[str | None] = mapped_column(Text, nullable=True)
