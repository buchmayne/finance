from pathlib import Path
from app.models import Base
from database import engine
from csv_importer import (
    process_all_csv_files,
    PathCSVDirectories,
    AccountType
)

def create_tables():
    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    create_tables()
    print("Database tables created")

    bank_account_dir = str(PathCSVDirectories.bank_accounts_dir)
    credit_cards_dir = str(PathCSVDirectories.credit_cards_dir)

    # Process all bank account csv files
    process_all_csv_files(
        folder_path=bank_account_dir,
        account_type=AccountType.BANK_ACCOUNT,
        dry_run=False,
    )

    # Process all credit card csv files
    process_all_csv_files(
        folder_path=credit_cards_dir,
        account_type=AccountType.CREDIT_CARD,
        dry_run=False,
    )

    print("Database Initialized")