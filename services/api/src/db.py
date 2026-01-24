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


# PushSubscription model for browser push notifications
class PushSubscription(Base):
    __tablename__ = "push_subscriptions"
    id = Column(Integer, primary_key=True, index=True)
    endpoint = Column(String, nullable=False, unique=True)
    p256dh = Column(String, nullable=False)  # Public key
    auth = Column(String, nullable=False)     # Auth secret
    created_at = Column(DateTime, default=datetime.utcnow)


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

#
Base.metadata.create_all(engine)
