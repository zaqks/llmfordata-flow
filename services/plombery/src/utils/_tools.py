from ._db import SessionLocal, Datasource, DatasourceAnalysis
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


# --- Functions for DatasourceAnalysis ---
def exists_analysis_by_datasource_id(datasource_id: int) -> bool:
    """Return True if a DatasourceAnalysis with the given datasource_id exists."""
    session = SessionLocal()
    try:
        return session.query(DatasourceAnalysis).filter(DatasourceAnalysis.datasource_id == datasource_id).first() is not None
    finally:
        session.close()


def insert_datasource_analysis(data: dict) -> bool:
    """Insert a DatasourceAnalysis record if the datasource_id is not present.

    `data` should contain at least: datasource_id. Other fields are optional.
    Returns True if inserted, False if skipped because of duplicate datasource_id.
    Raises on unexpected DB errors.
    """
    datasource_id = data.get("datasource_id")
    if datasource_id is None:
        raise ValueError("`datasource_id` is required to insert a DatasourceAnalysis")

    session = SessionLocal()
    try:
        # quick duplicate check
        if session.query(DatasourceAnalysis).filter(DatasourceAnalysis.datasource_id == datasource_id).first():
            return False

        analysis = DatasourceAnalysis(
            datasource_id=datasource_id,
            topics=data.get("topics"),
            keywords=data.get("keywords"),
            emerging_algorithms=data.get("emerging_algorithms"),
            summary=data.get("summary"),
            impact=data.get("impact"),
            exported=data.get("exported", False),
        )
        session.add(analysis)
        session.commit()
        return True
    except IntegrityError:
        session.rollback()
        return False
    finally:
        session.close()