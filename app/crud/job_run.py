import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.models.job_run_event import JobRunEvent
from app.models.job_run import JobRun


FINAL_STATUSES = {"completed", "failed", "cancelled"}


def create_job(
    db: Session,
    *,
    job_type: str,
    label: str,
    params: dict[str, Any] | None = None,
) -> JobRun:
    job = JobRun(
        job_type=job_type,
        label=label,
        status="queued",
        message="Queued",
        params_json=json.dumps(params or {}),
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


def get_job(db: Session, job_id: int) -> JobRun | None:
    return db.get(JobRun, job_id)


def list_jobs(db: Session, limit: int = 50) -> list[JobRun]:
    return db.query(JobRun).order_by(JobRun.queued_at.desc()).limit(limit).all()


def list_active_jobs(db: Session) -> list[JobRun]:
    return (
        db.query(JobRun)
        .filter(JobRun.status.in_(["queued", "running"]))
        .order_by(JobRun.queued_at.asc())
        .all()
    )


def get_next_queued_job(db: Session) -> JobRun | None:
    return (
        db.query(JobRun)
        .filter(JobRun.status == "queued")
        .order_by(JobRun.queued_at.asc())
        .first()
    )


def mark_running(db: Session, job: JobRun, *, message: str) -> JobRun:
    job.status = "running"
    job.cancel_requested = False
    job.started_at = datetime.now(tz=timezone.utc)
    job.message = message
    db.commit()
    db.refresh(job)
    return job


def mark_cancel_requested(db: Session, job: JobRun) -> JobRun:
    job.cancel_requested = True
    db.commit()
    db.refresh(job)
    return job


def mark_cancelled(db: Session, job: JobRun, *, message: str) -> JobRun:
    job.status = "cancelled"
    job.message = message
    job.completed_at = datetime.now(tz=timezone.utc)
    db.commit()
    db.refresh(job)
    return job


def create_event(db: Session, *, job_id: int, level: str, message: str) -> JobRunEvent:
    event = JobRunEvent(job_id=job_id, level=level, message=message)
    db.add(event)
    db.commit()
    db.refresh(event)
    return event


def list_events(db: Session, *, job_id: int, limit: int = 50) -> list[JobRunEvent]:
    return (
        db.query(JobRunEvent)
        .filter(JobRunEvent.job_id == job_id)
        .order_by(JobRunEvent.created_at.desc(), JobRunEvent.id.desc())
        .limit(limit)
        .all()
    )


def update_progress(
    db: Session,
    job: JobRun,
    *,
    message: str | None = None,
    done: int | None = None,
    total: int | None = None,
    stats: dict[str, Any] | None = None,
) -> JobRun:
    if message is not None:
        job.message = message
    if done is not None:
        job.progress_done = done
    if total is not None:
        job.progress_total = total
    if stats is not None:
        job.stats_json = json.dumps(stats)
    db.commit()
    db.refresh(job)
    return job


def mark_completed(db: Session, job: JobRun, *, message: str, stats: dict[str, Any] | None = None) -> JobRun:
    job.status = "completed"
    job.message = message
    job.completed_at = datetime.now(tz=timezone.utc)
    if stats is not None:
        job.stats_json = json.dumps(stats)
    db.commit()
    db.refresh(job)
    return job


def mark_failed(db: Session, job: JobRun, *, error: str, stats: dict[str, Any] | None = None) -> JobRun:
    job.status = "failed"
    job.message = "Failed"
    job.error = error
    job.completed_at = datetime.now(tz=timezone.utc)
    if stats is not None:
        job.stats_json = json.dumps(stats)
    db.commit()
    db.refresh(job)
    return job
