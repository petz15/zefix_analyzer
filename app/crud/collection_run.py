import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.models.collection_run import CollectionRun


def create_run(db: Session, run_type: str) -> CollectionRun:
    run = CollectionRun(run_type=run_type)
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def update_checkpoint(
    db: Session,
    run: CollectionRun,
    canton: str,
    offset: int,
    stats: dict[str, Any],
) -> CollectionRun:
    run.last_canton = canton
    run.last_offset = offset
    run.stats_json = json.dumps(stats)
    db.commit()
    db.refresh(run)
    return run


def complete_run(db: Session, run: CollectionRun, stats: dict[str, Any]) -> CollectionRun:
    run.completed_at = datetime.now(tz=timezone.utc)
    run.stats_json = json.dumps(stats)
    db.commit()
    db.refresh(run)
    return run


def get_last_incomplete_bulk(db: Session) -> CollectionRun | None:
    """Return the most recent incomplete bulk run, if any."""
    return (
        db.query(CollectionRun)
        .filter(CollectionRun.run_type == "bulk", CollectionRun.completed_at.is_(None))
        .order_by(CollectionRun.started_at.desc())
        .first()
    )
