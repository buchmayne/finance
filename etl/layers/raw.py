from abc import ABC, abstractmethod
import re
import hashlib
from pathlib import Path
import pandas as pd
from etl.database import get_db
from etl.types import AccountType, PathCSVDirectories
from etl.schema import create_raw_schema
from etl.models import BankAccountTransaction, CreditCardTransaction, Base
from etl.mappings import get_chase_bank_account_mapping, get_chase_credit_card_mapping

# Create raw tables
create_raw_schema()


class CSVImporter(ABC):
    def __init__(self):
        self.db = get_db()

    def preview(self, file_path: str) -> pd.DataFrame:
        """Preview CSV file structure"""
        df = pd.read_csv(file_path, nrows=5)
        print(f"\n📄 File: {Path(file_path).name}")
        print(f"📊 Shape: {df.shape}")
        print(f"📝 Columns: {list(df.columns)}")
        print("\n🔍 Sample data:")
        print(df.to_string())
        return df

    def _generate_id(self, row) -> str:
        """Generate unique ID for transaction"""
        # Create hash from date + amount + description
        content = f"{row['date']}{row['amount']}{str(row['description'])[:50]}"
        return hashlib.md5(content.encode()).hexdigest()[:12]

    def _extract_chase_account(self, file_path: str) -> str | None:
        patterns = [
            r"Chase\s*\d{4}",
            r"Chase.*?ending\s+in\s+(\d{4})",
            r"Chase.*?(\d{4})",
        ]

        for pattern in patterns:
            match = re.search(pattern, file_path, re.IGNORECASE)
            if match:
                return re.sub(r"\s+", "", match.group())

        return None

    def close(self):
        """Close database connection"""
        if self.db:
            self.db.close()

    @abstractmethod
    def import_csv(self):
        pass

    @abstractmethod
    def _clean_data(self):
        pass


class BankAccountCSVImporter(CSVImporter):

    def import_csv(
        self, file_path: str, column_mapping: dict, dry_run: bool = True
    ) -> pd.DataFrame:
        """Import CSV with column mapping"""

        # Read CSV
        df = pd.read_csv(file_path, usecols=range(1, 7))  # first column is NULL Details
        print(f"📥 Loaded {len(df)} rows from {Path(file_path).name}")

        # Apply column mapping
        df = df.rename(columns=column_mapping)

        # Basic data cleaning
        df = self._clean_data(df)

        # Extract account number
        account_number = self._extract_chase_account(file_path)

        if dry_run:
            print("🏃 DRY RUN - No data will be saved")
            print(f"Would import {len(df)} transactions:")
            print(df[["date", "amount", "description"]].head(10).to_string())
            return df

        # Save to database
        saved_count = 0
        for _, row in df.iterrows():
            transaction = BankAccountTransaction(
                id=self._generate_id(row),
                date=row["date"],
                amount=row["amount"],
                description=row["description"],
                balance=row["balance"],
                account=account_number,
                source_file=Path(file_path).name,
            )

            # Simple upsert logic
            existing = (
                self.db.query(BankAccountTransaction)
                .filter(BankAccountTransaction.id == transaction.id)
                .first()
            )
            if not existing:
                self.db.add(transaction)
                saved_count += 1

        self.db.commit()
        print(f"✅ Imported {saved_count} new transactions")
        return df

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Basic data cleaning"""

        # Parse dates
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        # Clean amounts (remove $ signs, commas)
        if "amount" in df.columns:
            df["amount"] = (
                df["amount"].astype(str).str.replace(r"[\$,]", "", regex=True)
            )
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

        # Fill missing descriptions
        if "description" in df.columns:
            df["description"] = df["description"].fillna("No description")

        return df


class CreditCardCSVImporter(CSVImporter):

    def import_csv(
        self, file_path: str, column_mapping: dict, dry_run: bool = True
    ) -> pd.DataFrame:
        """Import CSV with column mapping"""

        # Read CSV
        df = pd.read_csv(file_path)
        print(f"📥 Loaded {len(df)} rows from {Path(file_path).name}")

        # Apply column mapping
        df = df.rename(columns=column_mapping)

        # Basic data cleaning
        df = self._clean_data(df)

        # Extract card number
        card_number = self._extract_chase_account(file_path)

        if dry_run:
            print("🏃 DRY RUN - No data will be saved")
            print(f"Would import {len(df)} transactions:")
            print(df[["date", "amount", "description"]].head(10).to_string())
            return df

        # Save to database
        saved_count = 0
        for _, row in df.iterrows():
            transaction = CreditCardTransaction(
                id=self._generate_id(row),
                date=row["date"],
                amount=row["amount"],
                description=row["description"],
                category=row.get("category", "Other"),
                card_number=card_number,
                source_file=Path(file_path).name,
            )

            # Simple upsert logic
            existing = (
                self.db.query(CreditCardTransaction)
                .filter(CreditCardTransaction.id == transaction.id)
                .first()
            )
            if not existing:
                self.db.add(transaction)
                saved_count += 1

        self.db.commit()
        print(f"✅ Imported {saved_count} new transactions")
        return df

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Basic data cleaning"""

        # Parse dates
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        # Clean amounts (remove $ signs, commas)
        if "amount" in df.columns:
            df["amount"] = (
                df["amount"].astype(str).str.replace(r"[\$,]", "", regex=True)
            )
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

        # Fill missing descriptions
        if "description" in df.columns:
            df["description"] = df["description"].fillna("No description")

        # Add basic category if missing
        if "chase_category" not in df.columns:
            df["chase_category"] = "Other"

        return df


def process_csv_file(
    importer: CSVImporter, file_path: str, account_type: AccountType, dry_run: bool
) -> dict:
    """Process a single CSV file"""
    print(f"\n{'='*60}")
    print(f"📄 Processing: {file_path}")
    print(f"{'='*60}")

    try:
        if account_type == AccountType.BANK_ACCOUNT:
            column_mapping = get_chase_bank_account_mapping()
        elif account_type == AccountType.CREDIT_CARD:
            column_mapping = get_chase_credit_card_mapping()
        else:
            ValueError, "Account Type Not Supported"

        # Preview first
        print("👀 PREVIEW:")
        importer.preview(file_path)

        # Import
        print(f"\n🔄 IMPORTING with {account_type} mapping:")
        print(f"Column mapping: {column_mapping}")

        df = importer.import_csv(
            str(file_path), column_mapping=column_mapping, dry_run=dry_run
        )

        return {"file": file_path, "status": "success", "rows": len(df), "error": None}

    except Exception as e:
        print(f"❌ Error processing {file_path}: {e}")
        return {"file": file_path, "status": "error", "rows": 0, "error": str(e)}


def process_all_csv_files(
    folder_path: Path,
    account_type: AccountType,
    dry_run: bool,
    file_pattern: str = "*.csv",
) -> None:
    """Process all CSV files in a folder"""
    folder = Path(folder_path)

    if not folder.exists():
        print(f"❌ Folder does not exist: {folder}")
        return

    # Find all CSV files (case-insensitive)
    csv_files = []

    # Handle case-insensitive matching
    if file_pattern.lower() == "*.csv":
        # Default case: find both .csv and .CSV files
        csv_files.extend(folder.glob("*.csv"))
        csv_files.extend(folder.glob("*.CSV"))

    # Remove duplicates (in case a file matches multiple patterns)
    csv_files = list(set(csv_files))

    if not csv_files:
        print(f"❌ No CSV files found in {folder} matching pattern '{file_pattern}'")
        return

    print(f"📁 Found {len(csv_files)} CSV files in {folder}")
    print("Files to process:")
    for f in csv_files:
        print(f"  • {f.name}")

    # Process each file
    results = []

    if account_type == AccountType.BANK_ACCOUNT:
        importer = BankAccountCSVImporter()
    elif account_type == AccountType.CREDIT_CARD:
        importer = CreditCardCSVImporter()
    else:
        ValueError, "Account Type Not Supported"

    try:
        for csv_file in csv_files:
            result = process_csv_file(importer, csv_file, account_type, dry_run)
            results.append(result)
    finally:
        importer.close()

    # Summary report
    print(f"\n{'='*60}")
    print("📊 BATCH PROCESSING SUMMARY")
    print(f"{'='*60}")

    successful = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "error"]
    total_rows = sum(r["rows"] for r in successful)

    print(f"✅ Successful: {len(successful)} files")
    print(f"❌ Failed: {len(failed)} files")
    print(f"📈 Total rows processed: {total_rows}")

    if successful:
        print(f"\n✅ Successfully processed:")
        for result in successful:
            print(f"  • {result['file']}: {result['rows']} rows")

    if failed:
        print(f"\n❌ Failed files:")
        for result in failed:
            print(f"  • {result['file']}: {result['error']}")


def import_bank_accounts(dry_run: bool = False):
    """Raw layer function to import bank account CSVs"""
    process_all_csv_files(
        folder_path=str(PathCSVDirectories.bank_accounts_dir),
        account_type=AccountType.BANK_ACCOUNT,
        dry_run=dry_run,
    )


def import_credit_cards(dry_run: bool = False):
    """Raw layer function to import credit card CSVs"""
    process_all_csv_files(
        folder_path=str(PathCSVDirectories.credit_cards_dir),
        account_type=AccountType.CREDIT_CARD,
        dry_run=dry_run,
    )
