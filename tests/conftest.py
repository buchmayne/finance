"""Pytest configuration and shared fixtures for the test suite."""

import pytest
import pandas as pd
import tempfile
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from etl.models import Base


@pytest.fixture(scope="function")
def temp_db():
    """Create a temporary in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    yield session

    session.close()
    Base.metadata.drop_all(engine)


@pytest.fixture(scope="function")
def temp_db_connection():
    """Create a temporary database connection for pandas operations."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    connection = engine.connect()

    yield connection

    connection.close()


@pytest.fixture
def sample_bank_transactions_df():
    """Sample bank account transactions DataFrame for testing."""
    return pd.DataFrame({
        'date': pd.to_datetime(['2024-01-15', '2024-01-20', '2024-02-01']),
        'amount': [2500.00, -150.50, -45.00],
        'description': [
            'CLEARCOVER INC PAYROLL',
            'CHASE CREDIT CRD AUTOPAY',
            'VANGUARD BUY INVESTMENT'
        ],
        'balance': [5000.00, 4849.50, 4804.50],
        'account': ['Chase1234', 'Chase1234', 'Chase1234']
    })


@pytest.fixture
def sample_credit_card_transactions_df():
    """Sample credit card transactions DataFrame for testing."""
    return pd.DataFrame({
        'date': pd.to_datetime(['2024-01-10', '2024-01-15', '2024-02-05']),
        'amount': [-12.50, -45.00, -150.00],
        'description': [
            'SQ *OVATION COFFEE',
            'WHOLEFDS PRT 10148',
            'ALASKA AIR'
        ],
        'category': ['Food & Drink', 'Groceries', 'Travel']
    })


@pytest.fixture
def sample_bank_csv(tmp_path):
    """Create a sample Chase bank account CSV file."""
    csv_content = """Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/15/2024,CLEARCOVER INC PAYROLL,2500.00,CREDIT,5000.00,
DEBIT,01/20/2024,CHASE CREDIT CRD AUTOPAY,-150.50,DEBIT,4849.50,
DEBIT,02/01/2024,VANGUARD BUY INVESTMENT,-45.00,DEBIT,4804.50,
"""
    file_path = tmp_path / "Chase1234_bank.csv"
    file_path.write_text(csv_content)
    return file_path


@pytest.fixture
def sample_credit_card_csv(tmp_path):
    """Create a sample Chase credit card CSV file."""
    csv_content = """Transaction Date,Post Date,Description,Category,Type,Amount,Memo
01/10/2024,01/11/2024,SQ *OVATION COFFEE,Food & Drink,Sale,-12.50,
01/15/2024,01/16/2024,WHOLEFDS PRT 10148,Groceries,Sale,-45.00,
02/05/2024,02/06/2024,ALASKA AIR,Travel,Sale,-150.00,
"""
    file_path = tmp_path / "Chase5678_credit.csv"
    file_path.write_text(csv_content)
    return file_path


@pytest.fixture
def sample_staging_bank_df():
    """Sample staging bank account DataFrame with normalized data."""
    return pd.DataFrame({
        'id': ['abc123', 'def456', 'ghi789'],
        'date': pd.to_datetime(['2024-01-15', '2024-01-20', '2024-02-01']).date,
        'amount': [2500.00, -150.50, -45.00],
        'description': [
            'CLEARCOVER INC PAYROLL',
            'CHASE CREDIT CRD AUTOPAY',
            'VANGUARD BUY INVESTMENT'
        ],
        'balance': [5000.00, 4849.50, 4804.50],
        'account': ['Chase1234', 'Chase1234', 'Chase1234'],
        'category': ['SALARY', 'CREDIT_CARD_PAYMENT', 'TRANSFER_TO_BROKERAGE'],
        'year': [2024, 2024, 2024],
        'month': [1, 1, 2],
        'year_month': ['2024-01', '2024-01', '2024-02'],
        'day_of_week': [1, 6, 4],
        'source_file': ['test.csv', 'test.csv', 'test.csv'],
        'imported_at': pd.Timestamp.now()
    })


@pytest.fixture
def sample_staging_credit_card_df():
    """Sample staging credit card DataFrame with normalized data."""
    return pd.DataFrame({
        'id': ['xyz123', 'xyz456', 'xyz789'],
        'date': pd.to_datetime(['2024-01-10', '2024-01-15', '2024-02-05']).date,
        'amount': [-12.50, -45.00, -150.00],
        'description': [
            'SQ *OVATION COFFEE',
            'WHOLEFDS PRT 10148',
            'ALASKA AIR'
        ],
        'card_number': ['Chase5678', 'Chase5678', 'Chase5678'],
        'chase_category': ['Food & Drink', 'Groceries', 'Travel'],
        'category': ['OVATION', 'GROCERIES', 'FLIGHTS'],
        'year': [2024, 2024, 2024],
        'month': [1, 1, 2],
        'year_month': ['2024-01', '2024-01', '2024-02'],
        'day_of_week': [3, 1, 1],
        'source_file': ['test.csv', 'test.csv', 'test.csv'],
        'imported_at': pd.Timestamp.now()
    })


@pytest.fixture
def sample_marts_spending_df():
    """Sample marts spending DataFrame for testing metrics."""
    return pd.DataFrame({
        'id': ['s1', 's2', 's3', 's4'],
        'date': pd.to_datetime(['2024-01-10', '2024-01-15', '2024-02-05', '2024-02-10']).date,
        'amount': [12.50, 45.00, 150.00, 100.00],
        'description': ['OVATION COFFEE', 'WHOLE FOODS', 'ALASKA AIR', 'SAFEWAY'],
        'account_or_card_number': ['Chase5678', 'Chase5678', 'Chase5678', 'Chase5678'],
        'category': ['OVATION', 'GROCERIES', 'FLIGHTS', 'GROCERIES'],
        'meta_category': ['EATING_OUT', 'GROCERIES', 'TRAVEL', 'GROCERIES'],
        'year': [2024, 2024, 2024, 2024],
        'month': [1, 1, 2, 2],
        'year_month': ['2024-01', '2024-01', '2024-02', '2024-02'],
        'day_of_week': [3, 1, 1, 6],
        'source': ['credit_card', 'credit_card', 'credit_card', 'credit_card']
    })


@pytest.fixture
def sample_marts_income_df():
    """Sample marts income DataFrame for testing metrics."""
    return pd.DataFrame({
        'id': ['i1', 'i2'],
        'date': pd.to_datetime(['2024-01-15', '2024-02-15']).date,
        'amount': [2500.00, 2500.00],
        'description': ['CLEARCOVER INC PAYROLL', 'CLEARCOVER INC PAYROLL'],
        'account_or_card_number': ['Chase1234', 'Chase1234'],
        'category': ['SALARY', 'SALARY'],
        'year': [2024, 2024],
        'month': [1, 2],
        'year_month': ['2024-01', '2024-02'],
        'day_of_week': [1, 4],
        'source': ['bank_account', 'bank_account']
    })


@pytest.fixture
def sample_marts_savings_df():
    """Sample marts savings DataFrame for testing metrics."""
    return pd.DataFrame({
        'id': ['sv1', 'sv2'],
        'date': pd.to_datetime(['2024-01-20', '2024-02-20']).date,
        'amount': [500.00, 500.00],
        'description': ['VANGUARD BUY INVESTMENT', 'VANGUARD BUY INVESTMENT'],
        'account_or_card_number': ['Chase1234', 'Chase1234'],
        'category': ['TRANSFER_TO_BROKERAGE', 'TRANSFER_TO_BROKERAGE'],
        'year': [2024, 2024],
        'month': [1, 2],
        'year_month': ['2024-01', '2024-02'],
        'day_of_week': [6, 2],
        'source': ['bank_account', 'bank_account']
    })
