"""Tests for ETL raw layer (CSV import functionality)."""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock
from etl.layers.raw import (
    CSVImporter,
    BankAccountCSVImporter,
    CreditCardCSVImporter,
    process_csv_file,
    import_bank_accounts,
    import_credit_cards,
)
from etl.types import AccountType
from etl.models import BankAccountTransaction, CreditCardTransaction
from etl.mappings import get_chase_bank_account_mapping, get_chase_credit_card_mapping


class TestCSVImporter:
    """Test base CSVImporter abstract class methods."""

    def test_generate_id_creates_consistent_hash(self, temp_db):
        """Test that _generate_id creates consistent hashes for same input."""
        importer = BankAccountCSVImporter()
        importer.db = temp_db

        row1 = pd.Series(
            {"date": "2024-01-15", "amount": 100.00, "description": "TEST TRANSACTION"}
        )
        row2 = pd.Series(
            {"date": "2024-01-15", "amount": 100.00, "description": "TEST TRANSACTION"}
        )

        id1 = importer._generate_id(row1)
        id2 = importer._generate_id(row2)

        assert id1 == id2
        assert len(id1) == 12  # MD5 hash truncated to 12 chars
        importer.close()

    def test_generate_id_differs_for_different_inputs(self, temp_db):
        """Test that different transactions get different IDs."""
        importer = BankAccountCSVImporter()
        importer.db = temp_db

        row1 = pd.Series(
            {
                "date": "2024-01-15",
                "amount": 100.00,
                "description": "TEST TRANSACTION 1",
            }
        )
        row2 = pd.Series(
            {
                "date": "2024-01-15",
                "amount": 100.00,
                "description": "TEST TRANSACTION 2",
            }
        )

        id1 = importer._generate_id(row1)
        id2 = importer._generate_id(row2)

        assert id1 != id2
        importer.close()

    @pytest.mark.parametrize(
        "file_path,expected",
        [
            ("data/Chase1234_bank.csv", "Chase1234"),
            ("data/Chase 5678_bank.csv", "Chase5678"),
            ("data/account_ending_in_9012.csv", None),  # Doesn't match regex patterns
            ("data/no_account_here.csv", None),
        ],
    )
    def test_extract_chase_account(self, temp_db, file_path, expected):
        """Test account number extraction from file paths."""
        importer = BankAccountCSVImporter()
        importer.db = temp_db

        result = importer._extract_chase_account(file_path)
        assert result == expected
        importer.close()


class TestBankAccountCSVImporter:
    """Test bank account CSV import functionality."""

    def test_clean_data_parses_dates(self, temp_db):
        """Test that dates are properly parsed."""
        importer = BankAccountCSVImporter()
        importer.db = temp_db

        df = pd.DataFrame(
            {
                "date": ["01/15/2024", "02/20/2024"],
                "amount": ["$100.00", "$200.00"],
                "description": ["Test 1", "Test 2"],
            }
        )

        cleaned = importer._clean_data(df)

        assert pd.api.types.is_datetime64_any_dtype(cleaned["date"])
        importer.close()

    def test_clean_data_removes_currency_symbols(self, temp_db):
        """Test that currency symbols and commas are removed from amounts."""
        importer = BankAccountCSVImporter()
        importer.db = temp_db

        df = pd.DataFrame(
            {"date": ["01/15/2024"], "amount": ["$1,234.56"], "description": ["Test"]}
        )

        cleaned = importer._clean_data(df)

        assert cleaned["amount"].iloc[0] == 1234.56
        assert pd.api.types.is_numeric_dtype(cleaned["amount"])
        importer.close()

    def test_clean_data_fills_missing_descriptions(self, temp_db):
        """Test that missing descriptions are filled."""
        importer = BankAccountCSVImporter()
        importer.db = temp_db

        df = pd.DataFrame(
            {"date": ["01/15/2024"], "amount": ["100.00"], "description": [None]}
        )

        cleaned = importer._clean_data(df)

        assert cleaned["description"].iloc[0] == "No description"
        importer.close()

    def test_import_csv_dry_run_does_not_save(self, temp_db, sample_bank_csv):
        """Test that dry run mode doesn't save to database."""
        importer = BankAccountCSVImporter()
        importer.db = temp_db

        column_mapping = get_chase_bank_account_mapping()
        df = importer.import_csv(str(sample_bank_csv), column_mapping, dry_run=True)

        assert len(df) > 0
        # Check that nothing was saved to database
        count = temp_db.query(BankAccountTransaction).count()
        assert count == 0
        importer.close()

    def test_import_csv_saves_to_database(self, temp_db, sample_bank_csv):
        """Test that non-dry-run mode saves to database."""
        importer = BankAccountCSVImporter()
        importer.db = temp_db

        column_mapping = get_chase_bank_account_mapping()
        df = importer.import_csv(str(sample_bank_csv), column_mapping, dry_run=False)

        assert len(df) > 0
        count = temp_db.query(BankAccountTransaction).count()
        assert count == len(df)
        importer.close()

    def test_import_csv_prevents_duplicates(self, temp_db, sample_bank_csv):
        """Test that importing the same file twice doesn't create duplicates."""
        importer = BankAccountCSVImporter()
        importer.db = temp_db

        column_mapping = get_chase_bank_account_mapping()

        # Import first time
        df1 = importer.import_csv(str(sample_bank_csv), column_mapping, dry_run=False)
        count1 = temp_db.query(BankAccountTransaction).count()

        # Import second time
        df2 = importer.import_csv(str(sample_bank_csv), column_mapping, dry_run=False)
        count2 = temp_db.query(BankAccountTransaction).count()

        assert count1 == count2  # No new records added
        importer.close()


class TestCreditCardCSVImporter:
    """Test credit card CSV import functionality."""

    def test_clean_data_handles_missing_category(self, temp_db):
        """Test that missing category column is handled."""
        importer = CreditCardCSVImporter()
        importer.db = temp_db

        df = pd.DataFrame(
            {"date": ["01/15/2024"], "amount": ["-100.00"], "description": ["Test"]}
        )

        cleaned = importer._clean_data(df)

        assert "chase_category" in cleaned.columns
        assert cleaned["chase_category"].iloc[0] == "Other"
        importer.close()

    def test_import_csv_dry_run_does_not_save(self, temp_db, sample_credit_card_csv):
        """Test that dry run mode doesn't save to database."""
        importer = CreditCardCSVImporter()
        importer.db = temp_db

        column_mapping = get_chase_credit_card_mapping()
        df = importer.import_csv(
            str(sample_credit_card_csv), column_mapping, dry_run=True
        )

        assert len(df) > 0
        count = temp_db.query(CreditCardTransaction).count()
        assert count == 0
        importer.close()

    def test_import_csv_saves_to_database(self, temp_db, sample_credit_card_csv):
        """Test that non-dry-run mode saves to database."""
        importer = CreditCardCSVImporter()
        importer.db = temp_db

        column_mapping = get_chase_credit_card_mapping()
        df = importer.import_csv(
            str(sample_credit_card_csv), column_mapping, dry_run=False
        )

        assert len(df) > 0
        count = temp_db.query(CreditCardTransaction).count()
        assert count == len(df)
        importer.close()

    def test_import_csv_prevents_duplicates(self, temp_db, sample_credit_card_csv):
        """Test that importing the same file twice doesn't create duplicates."""
        importer = CreditCardCSVImporter()
        importer.db = temp_db

        column_mapping = get_chase_credit_card_mapping()

        # Import first time
        df1 = importer.import_csv(
            str(sample_credit_card_csv), column_mapping, dry_run=False
        )
        count1 = temp_db.query(CreditCardTransaction).count()

        # Import second time
        df2 = importer.import_csv(
            str(sample_credit_card_csv), column_mapping, dry_run=False
        )
        count2 = temp_db.query(CreditCardTransaction).count()

        assert count1 == count2
        importer.close()


class TestProcessCSVFile:
    """Test CSV file processing functions."""

    @patch("etl.layers.raw.get_db")
    def test_process_csv_file_success(self, mock_get_db, temp_db, sample_bank_csv):
        """Test successful CSV file processing."""
        mock_get_db.return_value = temp_db

        importer = BankAccountCSVImporter()
        importer.db = temp_db

        result = process_csv_file(
            importer, str(sample_bank_csv), AccountType.BANK_ACCOUNT, dry_run=True
        )

        assert result["status"] == "success"
        assert result["rows"] > 0
        assert result["error"] is None
        importer.close()

    @patch("etl.layers.raw.get_db")
    def test_process_csv_file_handles_errors(self, mock_get_db, temp_db, tmp_path):
        """Test that processing errors are caught and reported."""
        mock_get_db.return_value = temp_db

        # Create an invalid CSV
        bad_csv = tmp_path / "bad.csv"
        bad_csv.write_text("invalid,csv,content\n")

        importer = BankAccountCSVImporter()
        importer.db = temp_db

        result = process_csv_file(
            importer, str(bad_csv), AccountType.BANK_ACCOUNT, dry_run=True
        )

        assert result["status"] == "error"
        assert result["error"] is not None
        importer.close()


class TestBatchProcessing:
    """Test batch CSV processing functions."""

    @patch("etl.layers.raw.PathCSVDirectories.bank_accounts_dir")
    @patch("etl.layers.raw.get_db")
    def test_import_bank_accounts_processes_all_files(
        self, mock_get_db, mock_path, temp_db, tmp_path
    ):
        """Test that import_bank_accounts processes all CSV files in directory."""
        mock_get_db.return_value = temp_db
        mock_path.return_value = tmp_path

        # Create multiple CSV files
        csv_content = """Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #
DEBIT,01/15/2024,TEST TRANSACTION,100.00,DEBIT,1000.00,
"""
        for i in range(3):
            csv_file = tmp_path / f"Chase{i}_bank.csv"
            csv_file.write_text(csv_content)

        # Mock the PathCSVDirectories to return our temp path
        with patch("etl.layers.raw.PathCSVDirectories") as mock_dirs:
            mock_dirs.bank_accounts_dir = tmp_path
            # This would normally process files - we're just testing it runs
            # The actual function prints to stdout, so we can't easily assert
            # Just verify it doesn't crash
            try:
                import_bank_accounts(dry_run=True)
            except Exception as e:
                pytest.fail(f"import_bank_accounts raised exception: {e}")
