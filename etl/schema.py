from sqlalchemy import create_engine
from app.models import Base
from etl.config import Settings

def create_raw_schema():
    """Create raw data tables from SQLAlchemy models"""
    engine = create_engine(Settings.database_url)
    Base.metadata.create_all(engine)
    print("✅ Raw schema tables created")

def drop_all_tables():
    """Drop all tables - useful for development resets"""
    engine = create_engine(Settings.database_url)
    Base.metadata.drop_all(engine)
    print("🗑️ All tables dropped")