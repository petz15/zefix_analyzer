from collections import Counter
from datetime import date

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.company import Company
from app.schemas.company import CompanyCreate, CompanyUpdate

# Valid sort keys → (column_attr, ascending)
_SORT_MAP = {
    "name":             (Company.name,               True),
    "-name":            (Company.name,               False),
    "google_score":     (Company.website_match_score, True),
    "-google_score":    (Company.website_match_score, False),
    "zefix_score":      (Company.zefix_score,        True),
    "-zefix_score":     (Company.zefix_score,        False),
    "claude_score":     (Company.claude_score,       True),
    "-claude_score":    (Company.claude_score,       False),
    "tfidf_cluster":    (Company.tfidf_cluster,      True),
    "-tfidf_cluster":   (Company.tfidf_cluster,      False),
    "canton":           (Company.canton,             True),
    "-canton":          (Company.canton,             False),
    "status":           (Company.status,             True),
    "-status":          (Company.status,             False),
    "review_status":    (Company.review_status,      True),
    "-review_status":   (Company.review_status,      False),
    "proposal_status":  (Company.proposal_status,    True),
    "-proposal_status": (Company.proposal_status,    False),
    "website":          (Company.website_url,        True),
    "-website":         (Company.website_url,        False),
    "updated":          (Company.updated_at,         True),
    "-updated":         (Company.updated_at,         False),
    "created":          (Company.created_at,         True),
    "-created":         (Company.created_at,         False),
}
_DEFAULT_SORT = "-updated"


def get_company(db: Session, company_id: int) -> Company | None:
    return db.get(Company, company_id)


def get_company_by_uid(db: Session, uid: str) -> Company | None:
    return db.query(Company).filter(Company.uid == uid).first()


def _apply_filters(query, *, name_filter, canton, review_status, proposal_status,
                   google_searched, min_google_score, min_zefix_score, min_claude_score=None,
                   tags, tfidf_cluster=None, purpose_keywords=None):
    if name_filter:
        query = query.filter(Company.name.ilike(f"%{name_filter}%"))
    if canton:
        query = query.filter(Company.canton == canton)
    if review_status == "_none":
        query = query.filter(Company.review_status.is_(None))
    elif review_status:
        query = query.filter(Company.review_status == review_status)
    if proposal_status == "_none":
        query = query.filter(Company.proposal_status.is_(None))
    elif proposal_status:
        query = query.filter(Company.proposal_status == proposal_status)
    if google_searched == "yes":
        query = query.filter(Company.website_checked_at.isnot(None))
    elif google_searched == "no":
        query = query.filter(Company.website_checked_at.is_(None))
    elif google_searched == "no_result":
        query = query.filter(
            Company.website_checked_at.isnot(None),
            Company.website_url.is_(None),
        )
    if min_google_score is not None:
        query = query.filter(Company.website_match_score >= min_google_score)
    if min_zefix_score is not None:
        query = query.filter(Company.zefix_score >= min_zefix_score)
    if min_claude_score is not None:
        query = query.filter(Company.claude_score >= min_claude_score)
    if tags:
        query = query.filter(Company.tags.ilike(f"%{tags}%"))
    if tfidf_cluster == "_none":
        query = query.filter(Company.tfidf_cluster.is_(None))
    elif tfidf_cluster == "_any":
        query = query.filter(Company.tfidf_cluster.isnot(None))
    elif tfidf_cluster:
        query = query.filter(Company.tfidf_cluster.ilike(f"%{tfidf_cluster}%"))
    if purpose_keywords:
        query = query.filter(Company.purpose_keywords.ilike(f"%{purpose_keywords}%"))
    return query


def list_companies(
    db: Session,
    page: int = 1,
    page_size: int = 50,
    sort: str = _DEFAULT_SORT,
    name_filter: str | None = None,
    canton: str | None = None,
    review_status: str | None = None,
    proposal_status: str | None = None,
    google_searched: str | None = None,
    min_google_score: int | None = None,
    min_zefix_score: int | None = None,
    min_claude_score: int | None = None,
    tags: str | None = None,
    tfidf_cluster: str | None = None,
    purpose_keywords: str | None = None,
    # kept for backward-compat with collection.py batch query
    limit: int | None = None,
    skip: int = 0,
) -> list[Company]:
    query = db.query(Company)
    query = _apply_filters(
        query,
        name_filter=name_filter,
        canton=canton,
        review_status=review_status,
        proposal_status=proposal_status,
        google_searched=google_searched,
        min_google_score=min_google_score,
        min_zefix_score=min_zefix_score,
        min_claude_score=min_claude_score,
        tags=tags,
        tfidf_cluster=tfidf_cluster,
        purpose_keywords=purpose_keywords,
    )

    col, ascending = _SORT_MAP.get(sort, _SORT_MAP[_DEFAULT_SORT])
    query = query.order_by(col.asc() if ascending else col.desc())

    if limit is not None:
        # Legacy path used by batch collection
        return query.offset(skip).limit(limit).all()

    offset = (page - 1) * page_size
    return query.offset(offset).limit(page_size).all()


def count_companies(
    db: Session,
    name_filter: str | None = None,
    canton: str | None = None,
    review_status: str | None = None,
    proposal_status: str | None = None,
    google_searched: str | None = None,
    min_google_score: int | None = None,
    min_zefix_score: int | None = None,
    min_claude_score: int | None = None,
    tags: str | None = None,
    tfidf_cluster: str | None = None,
    purpose_keywords: str | None = None,
) -> int:
    query = db.query(Company)
    query = _apply_filters(
        query,
        name_filter=name_filter,
        canton=canton,
        review_status=review_status,
        proposal_status=proposal_status,
        google_searched=google_searched,
        min_google_score=min_google_score,
        min_zefix_score=min_zefix_score,
        min_claude_score=min_claude_score,
        tags=tags,
        tfidf_cluster=tfidf_cluster,
        purpose_keywords=purpose_keywords,
    )
    return query.count()


def get_company_stats(db: Session) -> dict:
    total = db.query(Company).count()
    searched = db.query(Company).filter(Company.website_checked_at.isnot(None)).count()
    with_website = db.query(Company).filter(Company.website_url.isnot(None)).count()

    # Google searches used today (by website_checked_at date)
    searches_today = (
        db.query(Company)
        .filter(func.date(Company.website_checked_at) == date.today())
        .count()
    )

    review_counts: dict[str, int] = {}
    for label in ("interesting", "rejected", "potential_proposal", "confirmed_proposal", "potential_generic", "confirmed_generic"):
        review_counts[label] = db.query(Company).filter(Company.review_status == label).count()
    review_counts["pending"] = db.query(Company).filter(Company.review_status.is_(None)).count()

    proposal_counts: dict[str, int] = {}
    for label in ("sent", "responded", "converted", "rejected"):
        proposal_counts[label] = db.query(Company).filter(Company.proposal_status == label).count()

    return {
        "total": total,
        "searched": searched,
        "with_website": with_website,
        "searches_today": searches_today,
        "review": review_counts,
        "proposal": proposal_counts,
    }


def get_taxonomy_stats(db: Session) -> dict:
    """Return distinct values + counts for tfidf_cluster and tags (sorted by count desc)."""
    # tfidf_cluster format: "label_a|label_b|label_c" where each label is "term1,term2,..."
    # Count how many companies belong to each cluster label (pipe-separated chunk)
    raw_clusters = (
        db.query(Company.tfidf_cluster)
        .filter(Company.tfidf_cluster.isnot(None))
        .filter(Company.tfidf_cluster != "Undefined")
        .all()
    )
    label_counter: Counter = Counter()
    for (val,) in raw_clusters:
        for label in val.split("|"):
            label = label.strip()
            if label:
                label_counter[label] += 1
    clusters_list = label_counter.most_common()

    # purpose_keywords is comma-separated per-company terms — count per individual keyword
    raw_keywords = (
        db.query(Company.purpose_keywords)
        .filter(Company.purpose_keywords.isnot(None))
        .all()
    )
    kw_counter: Counter = Counter()
    for (val,) in raw_keywords:
        for kw in val.split(","):
            kw = kw.strip()
            if kw:
                kw_counter[kw] += 1
    keywords_list = kw_counter.most_common()

    tags = (
        db.query(Company.tags, func.count(Company.id).label("cnt"))
        .filter(Company.tags.isnot(None))
        .group_by(Company.tags)
        .order_by(func.count(Company.id).desc())
        .all()
    )
    return {
        "clusters": clusters_list,
        "keywords": keywords_list,
        "tags": [(r.tags, r.cnt) for r in tags],
    }


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


def bulk_update_status(
    db: Session,
    company_ids: list[int],
    field: str,
    value: str | None,
) -> int:
    """Update a single status field on multiple companies at once. Returns updated count."""
    if field not in ("review_status", "proposal_status"):
        raise ValueError(f"bulk_update_status: unsupported field '{field}'")
    count = (
        db.query(Company)
        .filter(Company.id.in_(company_ids))
        .update({field: value}, synchronize_session=False)
    )
    db.commit()
    return count


def delete_company(db: Session, db_company: Company) -> None:
    db.delete(db_company)
    db.commit()
