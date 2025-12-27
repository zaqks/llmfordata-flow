from _db import SessionLocal, Datasource
from sqlalchemy.exc import IntegrityError


def exists_source_by_url(url: str) -> bool:
    """Return True if a datasource with `url` already exists."""
    session = SessionLocal()
    try:
        return session.query(Datasource).filter(Datasource.url == url).first() is not None
    finally:
        session.close()


def insert_datasource(data: dict) -> bool:
    """Insert a datasource record if the URL is not present.

    `data` should contain keys: source, external_id, title, abstract_or_summary,
    authors, date, url, tags

    Returns True if inserted, False if skipped because of duplicate url.
    Raises on unexpected DB errors.
    """
    url = data.get("url")
    if not url:
        raise ValueError("`url` is required to insert a datasource")

    session = SessionLocal()
    try:
        # quick duplicate check
        if session.query(Datasource).filter(Datasource.url == url).first():
            return False

        ds = Datasource(
            source=data.get("source"),
            external_id=data.get("id") or data.get("external_id"),
            title=data.get("title"),
            abstract_or_summary=data.get("abstract_or_summary"),
            authors=data.get("authors"),
            date=data.get("date"),
            url=url,
            tags=data.get("tags"),
        )
        session.add(ds)
        session.commit()
        return True
    except IntegrityError:
        session.rollback()
        return False
    finally:
        session.close()