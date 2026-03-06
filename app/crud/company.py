from sqlalchemy.orm import Session

from app.models.company import Company
from app.schemas.company import CompanyCreate, CompanyUpdate


def get_company(db: Session, company_id: int) -> Company | None:
    return db.get(Company, company_id)


def get_company_by_uid(db: Session, uid: str) -> Company | None:
    return db.query(Company).filter(Company.uid == uid).first()


def list_companies(db: Session, skip: int = 0, limit: int = 100, name_filter: str | None = None) -> list[Company]:
    query = db.query(Company)
    if name_filter:
        query = query.filter(Company.name.ilike(f"%{name_filter}%"))
    return query.order_by(Company.name).offset(skip).limit(limit).all()


def create_company(db: Session, company_in: CompanyCreate) -> Company:
    db_company = Company(**company_in.model_dump())
    db.add(db_company)
    db.commit()
    db.refresh(db_company)
    return db_company


def update_company(db: Session, db_company: Company, company_in: CompanyUpdate) -> Company:
    update_data = company_in.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_company, field, value)
    db.commit()
    db.refresh(db_company)
    return db_company


def delete_company(db: Session, db_company: Company) -> None:
    db.delete(db_company)
    db.commit()
