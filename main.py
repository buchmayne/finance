import argparse
import sys
from pathlib import Path
from csv_importer import (
    BankAccountCSVImporter,
    CreditCardCSVImporter,
    get_chase_bank_account_mapping,
    get_chase_credit_card_mapping,
)
from models import create_tables


def process_single_file(importer, file_path, account_type: str, dry_run):
    """Process a single CSV file"""
    print(f"\n{'='*60}")
    print(f"📄 Processing: {file_path}")
    print(f"{'='*60}")

    try:
        if account_type == "bank_account":
            column_mapping = get_chase_bank_account_mapping()
        elif account_type == "credit_card":
            column_mapping = get_chase_credit_card_mapping()
        else:
            ValueError, "Account Type Not Supported"

        # Preview first
        print("👀 PREVIEW:")
        importer.preview_csv(file_path)

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


def process_folder(folder_path, account_type, dry_run, file_pattern="*.csv"):
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

    if not dry_run:
        confirm = input(
            f"\n🚨 This will import {len(csv_files)} files to the database. Continue? (y/N): "
        )
        if confirm.lower() != "y":
            print("❌ Import cancelled")
            return

    # Process each file
    results = []

    if account_type == "bank_account":
        importer = BankAccountCSVImporter()
    elif account_type == "credit_card":
        importer = CreditCardCSVImporter()
    else:
        ValueError, "Account Type Not Supported"

    try:
        for csv_file in csv_files:
            result = process_single_file(importer, csv_file, account_type, dry_run)
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


def main():
    parser = argparse.ArgumentParser(description="Import CSV files to PostgreSQL")
    parser.add_argument("path", help="Path to CSV file or folder containing CSV files")
    parser.add_argument(
        "--account-type",
        choices=["bank_account", "credit_card"],
        help="Bank type for automatic column mapping",
    )
    parser.add_argument(
        "--preview-only", action="store_true", help="Just preview the file(s)"
    )
    parser.add_argument("--dry-run", action="store_true", help="Don't save to database")
    parser.add_argument(
        "--pattern", default="*.csv", help="File pattern to match (default: *.csv)"
    )

    args = parser.parse_args()

    # Ensure database tables exist
    create_tables()

    path = Path(args.path)

    # Determine if it's a file or folder
    if path.is_file():
        # Single file processing
        if args.account_type == "bank_account":
            importer = BankAccountCSVImporter()
        elif args.account_type == "credit_card":
            importer = CreditCardCSVImporter()
        else:
            ValueError, "Account Type Not Supported"

        try:
            if args.preview_only:
                importer.preview_csv(path.name)
                return

            result = process_single_file(
                importer, path, args.account_type, args.dry_run
            )
            if result["status"] == "error":
                sys.exit(1)
        finally:
            importer.close()

    elif path.is_dir():
        # Folder processing
        if args.preview_only:
            # Preview all files in folder
            csv_files = list(path.glob(args.pattern))
            if not csv_files:
                print(f"❌ No CSV files found matching pattern '{args.pattern}'")
                return

            if args.account_type == "bank_account":
                importer = BankAccountCSVImporter()
            elif args.account_type == "credit_card":
                importer = CreditCardCSVImporter()
            else:
                ValueError, "Account Type Not Supported"

            try:
                for csv_file in csv_files:
                    print(f"\n{'='*40}")
                    print(f"📄 Preview: {csv_file.name}")
                    print(f"{'='*40}")
                    importer.preview_csv(csv_file.name)
            finally:
                importer.close()
        else:
            process_folder(args.path, args.account_type, args.dry_run, args.pattern)
    else:
        print(f"❌ Path does not exist: {args.path}")
        sys.exit(1)


if __name__ == "__main__":
    main()
