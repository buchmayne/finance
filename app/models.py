from sqlalchemy import Column, String, Numeric, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

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
    category = Column(String(100))
    card_number = Column(String(10), nullable=True)
    source_file = Column(String(255))
    imported_at = Column(DateTime, default=datetime.now)


# Column mappings from raw csv files to db schema
def get_chase_credit_card_mapping():
    """Column mapping for Chase Credit Card CSV"""
    return {
        "Transaction Date": "date",
        "Post Date": "post_date",
        "Description": "description",
        "Category": "category",
        "Type": "transaction_type",
        "Amount": "amount",
        "Memo": "memo",
    }


def get_chase_bank_account_mapping():
    """Column mapping for Chase Bank Account CSV"""
    return {
        "Details": "details",
        "Posting Date": "date",
        "Description": "description",
        "Amount": "amount",
        "Type": "type",
        "Balance": "balance",
        "Check or Slip #": "check_number",
    }