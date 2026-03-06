"""Routes for company management and Zefix / Google Search integration."""

import json
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app import crud
from app.api import google_search_client, zefix_client
from app.database import get_db
from app.schemas.company import (
    CompanyCreate,
    CompanyRead,
    CompanyUpdate,
    GoogleSearchResult,
    ZefixSearchResult,
)

router = APIRouter(prefix="/companies", tags=["companies"])


# ---------------------------------------------------------------------------
# Zefix search (no DB persistence)
# ---------------------------------------------------------------------------


@router.get("/zefix/search", response_model=list[ZefixSearchResult], summary="Search Zefix API")
def zefix_search(
    name: str = Query(..., description="Company name to search for"),
    max_results: int = Query(20, ge=1, le=100, description="Maximum number of results"),
    active_only: bool = Query(False, description="Return only active companies"),
):
    """Query the Zefix REST API for Swiss companies matching *name*."""
    try:
        return zefix_client.search_companies(name, max_results=max_results, active_only=active_only)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc


@router.get("/zefix/{uid}", response_model=dict, summary="Get full Zefix company details")
def zefix_get_company(uid: str):
    """Fetch the full company record from the Zefix API by UID."""
    try:
        return zefix_client.get_company(uid)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Import from Zefix into DB
# ---------------------------------------------------------------------------


@router.post(
    "/zefix/import/{uid}",
    response_model=CompanyRead,
    status_code=status.HTTP_201_CREATED,
    summary="Import a company from Zefix into the database",
)
def import_from_zefix(uid: str, db: Session = Depends(get_db)):
    """Fetch a company from the Zefix API and store it in the local database.

    If the company (identified by UID) already exists, it is updated.
    """
    try:
        raw = zefix_client.get_company(uid)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    # Parse common fields from the raw response
    name_raw = raw.get("name", "")
    if isinstance(name_raw, dict):
        name = name_raw.get("de") or name_raw.get("fr") or name_raw.get("it") or next(iter(name_raw.values()), "")
    else:
        name = str(name_raw)

    legal_form_raw = raw.get("legalForm", {})
    if isinstance(legal_form_raw, dict):
        legal_form = legal_form_raw.get("de") or legal_form_raw.get("shortName") or None
    else:
        legal_form = str(legal_form_raw) if legal_form_raw else None

    address_parts = raw.get("address", {}) or {}
    address_str: str | None = None
    if isinstance(address_parts, dict):
        parts = [
            address_parts.get("street"),
            address_parts.get("houseNumber"),
            address_parts.get("swissZipCode"),
            address_parts.get("city"),
        ]
        address_str = " ".join(str(p) for p in parts if p) or None

    uid_normalised = zefix_client._normalise_uid(str(raw.get("uid", uid)))

    company_data = CompanyCreate(
        uid=uid_normalised,
        name=name,
        legal_form=legal_form,
        status=str(raw.get("status", "")) or None,
        municipality=raw.get("municipality") or None,
        canton=raw.get("canton") or None,
        purpose=raw.get("purpose") or None,
        address=address_str,
        zefix_raw=json.dumps(raw),
    )

    existing = crud.get_company_by_uid(db, uid_normalised)
    if existing:
        return crud.update_company(db, existing, CompanyUpdate(**company_data.model_dump(exclude={"uid"})))
    return crud.create_company(db, company_data)


# ---------------------------------------------------------------------------
# Google Search integration
# ---------------------------------------------------------------------------


@router.get(
    "/{company_id}/google-search",
    response_model=list[GoogleSearchResult],
    summary="Search Google for a company's website",
)
def google_search_for_company(
    company_id: int,
    num: int = Query(5, ge=1, le=10, description="Number of results"),
    db: Session = Depends(get_db),
):
    """Run a Google Custom Search for *company_id* and return the top results.

    The first result's URL is automatically saved as the company's ``website_url``.
    """
    db_company = crud.get_company(db, company_id)
    if not db_company:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Company not found")
    try:
        results = google_search_client.search_website(db_company.name, num=num)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    if results:
        crud.update_company(
            db,
            db_company,
            CompanyUpdate(website_url=results[0].link),
        )
        db_company.website_checked_at = datetime.now(tz=timezone.utc)
        db.commit()

    return results


# ---------------------------------------------------------------------------
# CRUD for companies in the DB
# ---------------------------------------------------------------------------


@router.get("", response_model=list[CompanyRead], summary="List companies")
def list_companies(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    name: str | None = Query(None, description="Filter by company name (case-insensitive)"),
    db: Session = Depends(get_db),
):
    return crud.list_companies(db, skip=skip, limit=limit, name_filter=name)


@router.post("", response_model=CompanyRead, status_code=status.HTTP_201_CREATED, summary="Create company")
def create_company(company_in: CompanyCreate, db: Session = Depends(get_db)):
    existing = crud.get_company_by_uid(db, company_in.uid)
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Company with this UID already exists")
    return crud.create_company(db, company_in)


@router.get("/{company_id}", response_model=CompanyRead, summary="Get company by ID")
def get_company(company_id: int, db: Session = Depends(get_db)):
    db_company = crud.get_company(db, company_id)
    if not db_company:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Company not found")
    return db_company


@router.patch("/{company_id}", response_model=CompanyRead, summary="Update company")
def update_company(company_id: int, company_in: CompanyUpdate, db: Session = Depends(get_db)):
    db_company = crud.get_company(db, company_id)
    if not db_company:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Company not found")
    return crud.update_company(db, db_company, company_in)


@router.delete("/{company_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete company")
def delete_company(company_id: int, db: Session = Depends(get_db)):
    db_company = crud.get_company(db, company_id)
    if not db_company:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Company not found")
    crud.delete_company(db, db_company)
