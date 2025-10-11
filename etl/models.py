from datetime import datetime
from typing import Type
from sqlalchemy import Column, String, Numeric, DateTime, Text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class BankAccountTransaction(Base):
    __tablename__ = "raw_bank_account_transactions"

    id = Column(String, primary_key=True)
    date = Column(DateTime, nullable=False)
    amount = Column(Numeric(12, 2), nullable=False)
    balance = Column(Numeric(12, 2), nullable=False)
    account = Column(String(10), nullable=True)
    description = Column(Text, nullable=True)
    source_file = Column(String(255))
    imported_at = Column(DateTime, default=datetime.now)


class CreditCardTransaction(Base):
    __tablename__ = "raw_credit_card_transactions"

    id = Column(String, primary_key=True)
    date = Column(DateTime, nullable=False)
    amount = Column(Numeric(12, 2), nullable=False)
    description = Column(Text, nullable=False)
    category = Column(String(100))
    card_number = Column(String(10), nullable=True)
    source_file = Column(String(255))
    imported_at = Column(DateTime, default=datetime.now)
