from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from app.models import (
    Base,
    BankAccountTransaction,
    CreditCardTransaction,
)
from config import Settings

# Database setup
engine = create_engine(Settings.database_url)
SessionLocal = sessionmaker(bind=engine)


def create_tables():
    """Create database tables"""
    Base.metadata.create_all(engine)
    print("✅ Database tables created")


def get_db():
    """Get database session"""
    return SessionLocal()


if __name__ == "__main__":
    create_tables()
    print("Database tables created")