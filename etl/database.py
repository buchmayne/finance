from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from etl.config import Settings

# Database setup
engine = create_engine(Settings.database_url)
SessionLocal = sessionmaker(bind=engine)


def get_db():
    """Get database session"""
    return SessionLocal()
