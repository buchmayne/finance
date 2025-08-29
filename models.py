from sqlalchemy import create_engine, Column, String, Numeric, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()


class BankAccountTransaction(Base):
    __tablename__ = "bank_account_transactions"

    id = Column(String, primary_key=True)
    date = Column(DateTime, nullable=False)
    amount = Column(Numeric(12, 2), nullable=False)
    balance = Column(Numeric(12, 2), nullable=False)
    account = Column(String(10), nullable=True)
    description = Column(Text, nullable=True)
    source_file = Column(String(255))
    imported_at = Column(DateTime, default=datetime.now)


class CreditCardTransaction(Base):
    __tablename__ = "credit_card_transactions"

    id = Column(String, primary_key=True)
    date = Column(DateTime, nullable=False)
    amount = Column(Numeric(12, 2), nullable=False)
    description = Column(Text, nullable=False)
    chase_category = Column(String(100))
    card_number = Column(String(10), nullable=True)
    source_file = Column(String(255))
    imported_at = Column(DateTime, default=datetime.now)


# Database setup
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


def create_tables():
    """Create database tables"""
    Base.metadata.create_all(engine)
    print("✅ Database tables created")


def get_db():
    """Get database session"""
    return SessionLocal()
