from pathlib import Path
from sqlalchemy import text
from app.models import Base
from database import engine, get_db
from csv_importer import (
    process_all_csv_files,
    PathCSVDirectories,
    AccountType
)
from queries import (
    create_credit_card_tx_clean,
    create_bank_account_tx_clean
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
    
    # Run ETL queries to create new tables
    db = get_db()
    
    db.execute(text(create_bank_account_tx_clean))
    db.execute(text(create_credit_card_tx_clean))

    db.close()

    