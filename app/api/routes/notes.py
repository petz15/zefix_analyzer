"""Routes for managing notes attached to companies."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app import crud
from app.database import get_db
from app.schemas.note import NoteCreate, NoteRead, NoteUpdate

router = APIRouter(prefix="/companies/{company_id}/notes", tags=["notes"])


@router.get("", response_model=list[NoteRead], summary="List notes for a company")
def list_notes(company_id: int, db: Session = Depends(get_db)):
    if not crud.get_company(db, company_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Company not found")
    return crud.list_notes_for_company(db, company_id)


@router.post("", response_model=NoteRead, status_code=status.HTTP_201_CREATED, summary="Add a note to a company")
def create_note(company_id: int, note_in: NoteCreate, db: Session = Depends(get_db)):
    if not crud.get_company(db, company_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Company not found")
    return crud.create_note(db, company_id, note_in)


@router.get("/{note_id}", response_model=NoteRead, summary="Get a specific note")
def get_note(company_id: int, note_id: int, db: Session = Depends(get_db)):
    db_note = crud.get_note(db, note_id)
    if not db_note or db_note.company_id != company_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Note not found")
    return db_note


@router.patch("/{note_id}", response_model=NoteRead, summary="Update a note")
def update_note(company_id: int, note_id: int, note_in: NoteUpdate, db: Session = Depends(get_db)):
    db_note = crud.get_note(db, note_id)
    if not db_note or db_note.company_id != company_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Note not found")
    return crud.update_note(db, db_note, note_in)


@router.delete("/{note_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a note")
def delete_note(company_id: int, note_id: int, db: Session = Depends(get_db)):
    db_note = crud.get_note(db, note_id)
    if not db_note or db_note.company_id != company_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Note not found")
    crud.delete_note(db, db_note)
