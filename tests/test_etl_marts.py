"""Tests for ETL marts layer (analytics table creation)."""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from etl.layers.marts import (
    INCOME_CATEGORIES,
    SAVINGS_CATEGORIES,
    _prepare_bank_account_tx_for_union,
    _prepare_credit_card_tx_for_union,
    create_unified_transactions,
    subset_transactions_on_savings,
    drop_savings_from_tx_tbl,
    drop_income_from_tx_tbl,
    assign_categories_to_meta_categories,
    create_transactions_tbl,
    create_spending_tbl,
    create_income_tbl,
    create_savings_tbl,
)


class TestDataPreparation:
    """Test data preparation functions for union operations."""

    def test_prepare_bank_account_tx_for_union(self):
        """Test bank account transaction preparation for union."""
        df = pd.DataFrame(
            {
                "id": ["1", "2", "3"],
                "account": ["Chase1234", "Chase1234", "Chase1234"],
                "amount": [100, -50, -25],
                "category": ["SALARY", "CREDIT_CARD_PAYMENT", "GROCERIES"],
                "balance": [1000, 950, 925],
                "source_file": ["test.csv", "test.csv", "test.csv"],
                "imported_at": [pd.Timestamp.now()] * 3,
                "description": ["Pay", "CC Payment", "Store"],
            }
        )

        result = _prepare_bank_account_tx_for_union(df)

        # Check excluded categories are filtered (TRANSFER_BETWEEN_CHASE_ACCOUNTS and CREDIT_CARD_PAYMENT)
        assert len(result) == 2  # SALARY and GROCERIES remain
        # Check columns are renamed
        assert "account_or_card_number" in result.columns
        assert "account" not in result.columns
        # Check dropped columns
        assert "balance" not in result.columns
        assert "source_file" not in result.columns
        assert "imported_at" not in result.columns
        # Check source column added
        assert "source" in result.columns
        assert result["source"].iloc[0] == "bank_account"

    def test_prepare_credit_card_tx_for_union(self):
        """Test credit card transaction preparation for union."""
        df = pd.DataFrame(
            {
                "id": ["1", "2"],
                "card_number": ["Chase5678", "Chase5678"],
                "amount": [-100, -50],
                "category": ["GROCERIES", "CREDIT_CARD_PAYMENT"],
                "chase_category": ["Groceries", "Payment"],
                "source_file": ["test.csv", "test.csv"],
                "imported_at": [pd.Timestamp.now()] * 2,
                "description": ["Store", "Payment"],
            }
        )

        result = _prepare_credit_card_tx_for_union(df)

        # Check excluded categories are filtered
        assert len(result) == 1  # Only GROCERIES remains
        # Check columns are renamed
        assert "account_or_card_number" in result.columns
        assert "card_number" not in result.columns
        # Check dropped columns
        assert "chase_category" not in result.columns
        assert "source_file" not in result.columns
        assert "imported_at" not in result.columns
        # Check source column added
        assert "source" in result.columns
        assert result["source"].iloc[0] == "credit_card"


class TestUnifiedTransactions:
    """Test unified transaction creation."""

    @patch("etl.layers.marts.get_db")
    def test_create_unified_transactions_combines_sources(
        self, mock_get_db, temp_db_connection
    ):
        """Test that bank and credit card transactions are properly combined."""
        mock_db = MagicMock()
        mock_db.connection.return_value = temp_db_connection
        mock_get_db.return_value = mock_db

        # Create mock staging tables
        bank_df = pd.DataFrame(
            {
                "id": ["b1"],
                "account": ["Chase1234"],
                "amount": [2500],
                "category": ["SALARY"],
                "balance": [5000],
                "source_file": ["test.csv"],
                "imported_at": [pd.Timestamp.now()],
                "description": ["Payroll"],
                "date": [pd.Timestamp("2024-01-15").date()],
                "year": [2024],
                "month": [1],
                "year_month": ["2024-01"],
                "day_of_week": [1],
            }
        )

        credit_df = pd.DataFrame(
            {
                "id": ["c1"],
                "card_number": ["Chase5678"],
                "amount": [-100],
                "category": ["GROCERIES"],
                "chase_category": ["Groceries"],
                "source_file": ["test.csv"],
                "imported_at": [pd.Timestamp.now()],
                "description": ["Store"],
                "date": [pd.Timestamp("2024-01-16").date()],
                "year": [2024],
                "month": [1],
                "year_month": ["2024-01"],
                "day_of_week": [2],
            }
        )

        bank_df.to_sql(
            "staging_bank_account_transactions",
            temp_db_connection,
            if_exists="replace",
            index=False,
        )
        credit_df.to_sql(
            "staging_credit_card_transactions",
            temp_db_connection,
            if_exists="replace",
            index=False,
        )

        result = create_unified_transactions()

        assert len(result) == 2
        assert "source" in result.columns
        assert set(result["source"].unique()) == {"bank_account", "credit_card"}
        assert "account_or_card_number" in result.columns


class TestTransactionFiltering:
    """Test transaction filtering functions."""

    def test_subset_transactions_on_savings(self):
        """Test that only savings transactions are returned."""
        df = pd.DataFrame(
            {
                "amount": [-500, 100, -50],
                "category": [
                    "TRANSFER_TO_BROKERAGE",
                    "SALARY",
                    "TRANSFER_FROM_BROKERAGE",
                ],
            }
        )

        result = subset_transactions_on_savings(df)

        assert len(result) == 2
        assert all(result["category"].isin(SAVINGS_CATEGORIES))
        # Check sign flip: transfer to brokerage should be positive (savings increase)
        assert (
            result[result["category"] == "TRANSFER_TO_BROKERAGE"]["amount"].iloc[0]
            == 500
        )

    def test_drop_savings_from_tx_tbl(self):
        """Test that savings transactions are excluded."""
        df = pd.DataFrame(
            {
                "amount": [-500, 100, -50],
                "category": ["TRANSFER_TO_BROKERAGE", "SALARY", "GROCERIES"],
            }
        )

        result = drop_savings_from_tx_tbl(df)

        assert len(result) == 2
        assert not any(result["category"].isin(SAVINGS_CATEGORIES))

    def test_drop_income_from_tx_tbl(self):
        """Test that income transactions are excluded."""
        df = pd.DataFrame(
            {"amount": [2500, -100, -50], "category": ["SALARY", "GROCERIES", "GAS"]}
        )

        result = drop_income_from_tx_tbl(df)

        assert len(result) == 2
        assert not any(result["category"].isin(INCOME_CATEGORIES))


class TestMetaCategories:
    """Test meta category assignment."""

    @pytest.mark.parametrize(
        "category,expected_meta",
        [
            ("MORTGAGE_PAYMENT", "HOUSING"),
            ("HOA_PAYMENT", "HOUSING"),
            ("WEDDING", "WEDDING"),
            ("JENNA_WEDDING_ACCT_TRANSFERS", "WEDDING"),
            ("SPOTIFY_MEMBERSHIP", "ENTERTAINMENT_SUBSCRIPTIONS"),
            ("HBO_SUBSCRIPTION", "ENTERTAINMENT_SUBSCRIPTIONS"),
            ("SALARY", "INCOME"),
            ("TAX_REFUND", "INCOME"),
            ("CASH_WITHDRAWL", "CASH_WITHDRAWL"),
            ("CAR_INSURANCE", "INSURANCE"),
            ("DIAMOND_INSURANCE", "INSURANCE"),
            ("CELL_PHONE_BILL", "UTILITIES"),
            ("COMCAST", "UTILITIES"),
            ("PGE", "UTILITIES"),
            ("FAST_FOOD", "EATING_OUT"),
            ("EATING_OUT", "EATING_OUT"),
            ("OVATION_WEEKDAY", "EATING_OUT"),
            ("GROCERIES", "GROCERIES"),
            ("FLIGHTS", "TRAVEL"),
            ("TRAVEL_LODGING", "TRAVEL"),
            ("VOD_AMAZON", "MOVIES"),
            ("MOVIES", "MOVIES"),
            ("POWELLS", "HOBBY_PHYSICAL_MEDIA"),
            ("MTG", "HOBBY_PHYSICAL_MEDIA"),
            ("CONCERTS", "CONCERTS_AND_SPORTING_EVENTS"),
            ("MODA_CENTER", "CONCERTS_AND_SPORTING_EVENTS"),
            ("LIQUOR_STORE", "HOBBY_COCKTAILS"),
            ("GYM_MEMBERSHIP", "HOBBY_SPORTS"),
            ("INDOOR_SOCCER", "HOBBY_SPORTS"),
            ("CLOTHES", "CLOTHES"),
            ("DRY_CLEANING", "CLOTHES"),
            ("GAS", "CAR"),
            ("CAR_MAINTENANCE", "CAR"),
            ("VENMO_PAYMENT", "VENMO"),
            ("HOSTING_SOFTWARE_PROJECTS", "HOBBY_TECH"),
            ("COMPUTERS_TECHNOLOGY_HARDWARE", "HOBBY_TECH"),
            ("AMAZON_PURCHASE", "AMAZON_SPENDING"),
            ("RANDOM_CATEGORY", "OTHER"),
        ],
    )
    def test_assign_categories_to_meta_categories(self, category, expected_meta):
        """Test that categories are correctly mapped to meta categories."""
        df = pd.DataFrame({"category": [category], "amount": [100]})

        result = assign_categories_to_meta_categories(df)

        assert "meta_category" in result.columns
        assert result["meta_category"].iloc[0] == expected_meta

    def test_assign_categories_to_meta_categories_handles_multiple_rows(self):
        """Test meta category assignment with multiple transactions."""
        df = pd.DataFrame(
            {
                "category": [
                    "SALARY",
                    "GROCERIES",
                    "FLIGHTS",
                    "CONCERTS",
                    "AMAZON_PURCHASE",
                ],
                "amount": [2500, 100, 500, 50, 75],
            }
        )

        result = assign_categories_to_meta_categories(df)

        assert len(result) == 5
        assert result["meta_category"].tolist() == [
            "INCOME",
            "GROCERIES",
            "TRAVEL",
            "CONCERTS_AND_SPORTING_EVENTS",
            "AMAZON_SPENDING",
        ]


class TestMartsTableCreation:
    """Test marts table creation functions."""

    @patch("etl.layers.marts.get_db")
    @patch("etl.layers.marts.create_unified_transactions")
    def test_create_transactions_tbl(
        self, mock_unified, mock_get_db, temp_db_connection
    ):
        """Test that marts_transactions table is created correctly."""
        # Mock unified transactions
        mock_unified.return_value = pd.DataFrame(
            {
                "id": ["1", "2"],
                "amount": [2500, -100],
                "category": ["SALARY", "GROCERIES"],
                "account_or_card_number": ["Chase1234", "Chase5678"],
                "description": ["Pay", "Store"],
                "date": [
                    pd.Timestamp("2024-01-15").date(),
                    pd.Timestamp("2024-01-16").date(),
                ],
                "year": [2024, 2024],
                "month": [1, 1],
                "year_month": ["2024-01", "2024-01"],
                "day_of_week": [1, 2],
                "source": ["bank_account", "credit_card"],
            }
        )

        mock_db = MagicMock()
        mock_db.connection.return_value = temp_db_connection
        mock_get_db.return_value = mock_db

        create_transactions_tbl()

        # Verify table was created
        result = pd.read_sql("SELECT * FROM marts_transactions", temp_db_connection)
        assert len(result) > 0
        assert "meta_category" in result.columns

    @patch("etl.layers.marts.get_db")
    @patch("etl.layers.marts.create_unified_transactions")
    def test_create_spending_tbl_excludes_income(
        self, mock_unified, mock_get_db, temp_db_connection
    ):
        """Test that marts_spending table excludes income and savings."""
        mock_unified.return_value = pd.DataFrame(
            {
                "id": ["1", "2", "3"],
                "amount": [2500, -100, -500],
                "category": ["SALARY", "GROCERIES", "TRANSFER_TO_BROKERAGE"],
                "account_or_card_number": ["Chase1234", "Chase5678", "Chase1234"],
                "description": ["Pay", "Store", "Investment"],
                "date": [pd.Timestamp("2024-01-15").date()] * 3,
                "year": [2024, 2024, 2024],
                "month": [1, 1, 1],
                "year_month": ["2024-01", "2024-01", "2024-01"],
                "day_of_week": [1, 2, 3],
                "source": ["bank_account", "credit_card", "bank_account"],
            }
        )

        mock_db = MagicMock()
        mock_db.connection.return_value = temp_db_connection
        mock_get_db.return_value = mock_db

        create_spending_tbl()

        result = pd.read_sql("SELECT * FROM marts_spending", temp_db_connection)
        # Only GROCERIES should remain
        assert len(result) == 1
        assert result["category"].iloc[0] == "GROCERIES"
        # Amount should be positive (spending flipped from negative)
        assert result["amount"].iloc[0] == 100

    @patch("etl.layers.marts.get_db")
    @patch("etl.layers.marts.create_unified_transactions")
    def test_create_income_tbl_only_includes_income(
        self, mock_unified, mock_get_db, temp_db_connection
    ):
        """Test that marts_income table only includes income categories."""
        mock_unified.return_value = pd.DataFrame(
            {
                "id": ["1", "2", "3"],
                "amount": [2500, -100, 50],
                "category": ["SALARY", "GROCERIES", "TAX_REFUND"],
                "account_or_card_number": ["Chase1234", "Chase5678", "Chase1234"],
                "description": ["Pay", "Store", "Refund"],
                "date": [pd.Timestamp("2024-01-15").date()] * 3,
                "year": [2024, 2024, 2024],
                "month": [1, 1, 1],
                "year_month": ["2024-01", "2024-01", "2024-01"],
                "day_of_week": [1, 2, 3],
                "source": ["bank_account", "credit_card", "bank_account"],
            }
        )

        mock_db = MagicMock()
        mock_db.connection.return_value = temp_db_connection
        mock_get_db.return_value = mock_db

        create_income_tbl()

        result = pd.read_sql("SELECT * FROM marts_income", temp_db_connection)
        # Only SALARY and TAX_REFUND should remain
        assert len(result) == 2
        assert set(result["category"].unique()) == {"SALARY", "TAX_REFUND"}

    @patch("etl.layers.marts.get_db")
    @patch("etl.layers.marts.create_unified_transactions")
    def test_create_savings_tbl_only_includes_savings(
        self, mock_unified, mock_get_db, temp_db_connection
    ):
        """Test that marts_savings table only includes savings categories."""
        mock_unified.return_value = pd.DataFrame(
            {
                "id": ["1", "2", "3"],
                "amount": [2500, -100, -500],
                "category": ["SALARY", "GROCERIES", "TRANSFER_TO_BROKERAGE"],
                "account_or_card_number": ["Chase1234", "Chase5678", "Chase1234"],
                "description": ["Pay", "Store", "Investment"],
                "date": [pd.Timestamp("2024-01-15").date()] * 3,
                "year": [2024, 2024, 2024],
                "month": [1, 1, 1],
                "year_month": ["2024-01", "2024-01", "2024-01"],
                "day_of_week": [1, 2, 3],
                "source": ["bank_account", "credit_card", "bank_account"],
            }
        )

        mock_db = MagicMock()
        mock_db.connection.return_value = temp_db_connection
        mock_get_db.return_value = mock_db

        create_savings_tbl()

        result = pd.read_sql("SELECT * FROM marts_savings", temp_db_connection)
        # Only TRANSFER_TO_BROKERAGE should remain
        assert len(result) == 1
        assert result["category"].iloc[0] == "TRANSFER_TO_BROKERAGE"
        # Amount should be positive (savings increase)
        assert result["amount"].iloc[0] == 500


class TestConstants:
    """Test that constants are properly defined."""

    def test_income_categories_defined(self):
        """Test that INCOME_CATEGORIES constant is defined."""
        assert isinstance(INCOME_CATEGORIES, list)
        assert len(INCOME_CATEGORIES) > 0
        assert "SALARY" in INCOME_CATEGORIES

    def test_savings_categories_defined(self):
        """Test that SAVINGS_CATEGORIES constant is defined."""
        assert isinstance(SAVINGS_CATEGORIES, list)
        assert len(SAVINGS_CATEGORIES) > 0
        assert "TRANSFER_TO_BROKERAGE" in SAVINGS_CATEGORIES
