import re

from sqlalchemy.orm import Session

from app.models.boilerplate import BoilerplatePattern


def list_boilerplate_patterns(db: Session) -> list[BoilerplatePattern]:
    return db.query(BoilerplatePattern).order_by(BoilerplatePattern.id).all()


def get_active_boilerplate_patterns(db: Session) -> list[re.Pattern]:
    """Return compiled regex patterns for all active boilerplate entries."""
    rows = db.query(BoilerplatePattern).filter(BoilerplatePattern.active.is_(True)).all()
    compiled = []
    for row in rows:
        try:
            compiled.append(re.compile(row.pattern, re.IGNORECASE))
        except re.error:
            pass  # skip invalid patterns silently
    return compiled


def create_boilerplate_pattern(
    db: Session,
    *,
    pattern: str,
    description: str | None = None,
    example: str | None = None,
    match_count: int | None = None,
    active: bool = True,
) -> BoilerplatePattern:
    row = BoilerplatePattern(
        pattern=pattern,
        description=description,
        example=example,
        match_count=match_count,
        active=active,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def update_boilerplate_pattern(
    db: Session,
    row: BoilerplatePattern,
    *,
    pattern: str | None = None,
    description: str | None = None,
    example: str | None = None,
    active: bool | None = None,
) -> BoilerplatePattern:
    if pattern is not None:
        row.pattern = pattern
    if description is not None:
        row.description = description
    if example is not None:
        row.example = example
    if active is not None:
        row.active = active
    db.commit()
    db.refresh(row)
    return row


def delete_boilerplate_pattern(db: Session, row: BoilerplatePattern) -> None:
    db.delete(row)
    db.commit()


def get_boilerplate_pattern(db: Session, pattern_id: int) -> BoilerplatePattern | None:
    return db.query(BoilerplatePattern).filter(BoilerplatePattern.id == pattern_id).first()
