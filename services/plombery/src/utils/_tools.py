from ._db import SessionLocal, Datasource, DatasourceAnalysis, Reports, Documents
from sqlalchemy.exc import IntegrityError

# --- Utility function to save report and documents ---
def save_report_and_documents(output_dir, session=None):
    """
    Creates a Reports entry with the timestamp from the output_dir name, and saves Documents for all relevant files in the directory.
    Args:
        output_dir (str): Path to the output directory (e.g., /tmp/report_YYYYMMDD_HHMMSS)
        session: Optional SQLAlchemy session. If None, a new session is created and closed inside.
    Returns:
        report_id (int): The ID of the created Reports entry.
    """
    import os
    from datetime import datetime

    own_session = False
    if session is None:
        session = SessionLocal()
        own_session = True
    try:
        # Extract timestamp from folder name (assumes /tmp/report_YYYYMMDD_HHMMSS)
        folder_name = os.path.basename(output_dir)
        try:
            ts_str = folder_name.split('_', 1)[-1]
            created_at = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
        except Exception:
            created_at = datetime.utcnow()

        # Create Reports entry
        report = Reports(created_at=created_at)
        session.add(report)
        session.flush()  # Get report.id

        # File types to save: .md, .png, .jpg, .jpeg, .svg
        valid_exts = {".md", ".png", ".jpg", ".jpeg", ".svg"}
        for fname in os.listdir(output_dir):
            fpath = os.path.join(output_dir, fname)
            if not os.path.isfile(fpath):
                continue
            ext = os.path.splitext(fname)[1].lower()
            if ext not in valid_exts:
                continue
            with open(fpath, "rb") as f:
                file_bytes = f.read()
            doc = Documents(name=fname, file=file_bytes, report_id=report.id)
            session.add(doc)

        session.commit()
        return report.id
    finally:
        if own_session:
            session.close()


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

# non exported stuff
def fetch_analysis_rows(session, num_rows):
    return session.query(DatasourceAnalysis).filter(DatasourceAnalysis.exported == False).order_by(DatasourceAnalysis.id).limit(num_rows).all()
    # return session.query(DatasourceAnalysis).order_by(DatasourceAnalysis.id).limit(num_rows).all()
