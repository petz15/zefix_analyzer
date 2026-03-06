from sqlalchemy.orm import Session

from app.models.note import Note
from app.schemas.note import NoteCreate, NoteUpdate


def get_note(db: Session, note_id: int) -> Note | None:
    return db.get(Note, note_id)


def list_notes_for_company(db: Session, company_id: int) -> list[Note]:
    return (
        db.query(Note)
        .filter(Note.company_id == company_id)
        .order_by(Note.created_at.desc())
        .all()
    )


def create_note(db: Session, company_id: int, note_in: NoteCreate) -> Note:
    db_note = Note(company_id=company_id, **note_in.model_dump())
    db.add(db_note)
    db.commit()
    db.refresh(db_note)
    return db_note


def update_note(db: Session, db_note: Note, note_in: NoteUpdate) -> Note:
    db_note.content = note_in.content
    db.commit()
    db.refresh(db_note)
    return db_note


def delete_note(db: Session, db_note: Note) -> None:
    db.delete(db_note)
    db.commit()
