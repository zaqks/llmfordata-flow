import os
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, LargeBinary, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


# Reports model
class Reports(Base):
    __tablename__ = "reports"
    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)


# Documents model
class Documents(Base):
    __tablename__ = "documents"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    file = Column(LargeBinary, nullable=False)
    report_id = Column(Integer, ForeignKey("reports.id"), nullable=True)


# Database connection
engine = create_engine(os.getenv("DATABASE_URL"))  # SQLite database file

# Session factory
SessionLocal = sessionmaker(bind=engine)


def get_db():
    """Dependency to get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
