"""Tests for ETL staging layer (data cleaning and categorization)."""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from etl.layers.staging import (
    _normalize_description,
    _normalize_date,
    _categorize_individual_bank_transaction,
    _categorize_all_bank_transactions,
    _categorize_individual_credit_card_transaction,
    _categorize_all_credit_cards_transactions,
    _rename_chase_category_col,
    _update_credit_card_transactions_categories,
    clean_bank_account_data,
    clean_credit_card_data,
    create_staging_bank_account_transactions,
    create_staging_credit_card_transactions,
)


class TestNormalizeDescription:
    """Test description normalization."""

    def test_normalize_description_converts_to_uppercase(self):
        """Test that descriptions are converted to uppercase."""
        df = pd.DataFrame({"description": ["test transaction", "Another Test"]})

        result = _normalize_description(df)

        assert result["description"].iloc[0] == "TEST TRANSACTION"
        assert result["description"].iloc[1] == "ANOTHER TEST"

    def test_normalize_description_removes_extra_whitespace(self):
        """Test that extra whitespace is removed."""
        df = pd.DataFrame({"description": ["test   transaction", "  another  test  "]})

        result = _normalize_description(df)

        assert result["description"].iloc[0] == "TEST TRANSACTION"
        assert result["description"].iloc[1] == "ANOTHER TEST"

    def test_normalize_description_strips_leading_trailing(self):
        """Test that leading/trailing whitespace is stripped."""
        df = pd.DataFrame({"description": ["  test  ", "\ttest\n"]})

        result = _normalize_description(df)

        assert result["description"].iloc[0] == "TEST"
        assert result["description"].iloc[1] == "TEST"


class TestNormalizeDate:
    """Test date normalization and temporal column creation."""

    def test_normalize_date_creates_date_column(self):
        """Test that date column is properly converted."""
        df = pd.DataFrame({"date": pd.to_datetime(["2024-01-15", "2024-02-20"])})

        result = _normalize_date(df)

        assert "date" in result.columns
        # Check that dates are date objects, not datetime
        assert isinstance(
            result["date"].iloc[0],
            (
                pd.Timestamp,
                pd.DatetimeTZDtype,
                type(pd.to_datetime("2024-01-15").date()),
            ),
        )

    def test_normalize_date_creates_temporal_columns(self):
        """Test that year, month, year_month columns are created."""
        df = pd.DataFrame({"date": pd.to_datetime(["2024-01-15", "2024-12-25"])})

        result = _normalize_date(df)

        assert "year" in result.columns
        assert "month" in result.columns
        assert "year_month" in result.columns
        assert "day_of_week" in result.columns

        assert result["year"].iloc[0] == 2024
        assert result["month"].iloc[0] == 1
        assert result["year_month"].iloc[0] == "2024-01"
        assert result["month"].iloc[1] == 12
        assert result["year_month"].iloc[1] == "2024-12"

    def test_normalize_date_day_of_week_monday_is_1(self):
        """Test that day_of_week starts with Monday=1."""
        # 2024-01-15 is a Monday
        df = pd.DataFrame({"date": pd.to_datetime(["2024-01-15"])})

        result = _normalize_date(df)

        assert result["day_of_week"].iloc[0] == 1

    def test_normalize_date_day_of_week_sunday_is_7(self):
        """Test that Sunday is day 7."""
        # 2024-01-14 is a Sunday
        df = pd.DataFrame({"date": pd.to_datetime(["2024-01-14"])})

        result = _normalize_date(df)

        assert result["day_of_week"].iloc[0] == 7


class TestCategorizeBankTransactions:
    """Test bank transaction categorization."""

    @pytest.mark.parametrize(
        "description,expected_category",
        [
            ("CLEARCOVER INC PAYROLL", "SALARY"),
            ("FEDEX DATAWORKS DIR DEP", "SALARY"),
            ("INTEREST PAYMENT", "ACCOUNT_INTEREST"),
            ("VERIZON WIRELESS PAYMENTS", "CELL_PHONE_BILL"),
            ("VANGUARD BUY INVESTMENT", "TRANSFER_TO_BROKERAGE"),
            ("VANGUARD SELL INVESTMENT", "TRANSFER_FROM_BROKERAGE"),
            ("VENMO PAYMENT", "VENMO_PAYMENT"),
            ("VENMO CASHOUT", "VENMO_CASHOUT"),
            ("PINNACLE COA", "HOA_PAYMENT"),
            ("CHASE CREDIT CRD AUTOPAY", "CREDIT_CARD_PAYMENT"),
            ("ONPOINT COMMUNIT RE PAYMENT", "MORTGAGE_PAYMENT"),
            ("ONLINE TRANSFER TO SAV", "TRANSFER_BETWEEN_CHASE_ACCOUNTS"),
            ("DEPOSIT ID NUMBER 12345", "CASH_DEPOSIT"),
            ("WITHDRAWAL 07/14", "CASH_WITHDRAWL_FOR_WEDDING"),
            ("WITHDRAWAL SOMETHING", "CASH_WITHDRAWL"),
            ("ALEX ELISE", "WEDDING_PHOTOGRAPHER"),
            ("WEX HEALTH PREMIUMS 28670940 WEB ID", "COBRA_PAYMENTS"),
            ("OR REVENUE DEPT ORSTTAXRFD", "TAX_REFUND"),
            ("IRS TREAS 310 TAX REF", "TAX_REFUND"),
            ("CHECK # 1976 PASSPORTSERVICES PAYMENT ARC ID", "PASSPORT_RENEWAL"),
            ("RANDOM TRANSACTION", "OTHER"),
        ],
    )
    def test_categorize_individual_bank_transaction(
        self, description, expected_category
    ):
        """Test bank transaction categorization logic."""
        result = _categorize_individual_bank_transaction(description)
        assert result == expected_category

    def test_categorize_all_bank_transactions_adds_category_column(self):
        """Test that categorization adds a category column to DataFrame."""
        df = pd.DataFrame(
            {
                "description": [
                    "CLEARCOVER INC PAYROLL",
                    "VANGUARD BUY INVESTMENT",
                    "RANDOM TRANSACTION",
                ]
            }
        )

        result = _categorize_all_bank_transactions(df)

        assert "category" in result.columns
        assert result["category"].iloc[0] == "SALARY"
        assert result["category"].iloc[1] == "TRANSFER_TO_BROKERAGE"
        assert result["category"].iloc[2] == "OTHER"


class TestCategorizeCreditCardTransactions:
    """Test credit card transaction categorization."""

    @pytest.mark.parametrize(
        "description,expected_category",
        [
            ("PAYMENT THANK YOU-MOBILE", "CREDIT_CARD_PAYMENT"),
            ("SQ *OVATION COFFEE", "OVATION"),
            ("SQ *COFFEE TIME", "OTHER_COFFEE_SHOPS"),
            ("CHIPOTLE ONLINE", "EATING_OUT_NBHD_LUNCH"),
            ("DOMINO PIZZA", "DOMINOS"),
            ("HAWTHORNE THEATER", "CONCERTS"),
            ("PAYMASTER LOUNGE", "NBHD_BARS"),
            ("LYFT *RIDE", "RIDESHARE"),
            ("UBER *TRIP", "RIDESHARE"),
            ("SAFEWAY #2790", "GROCERIES"),
            ("LIQUOR STORE", "LIQUOR_STORE"),
            ("PORTLAND GENERAL ELECTRIC", "PGE"),
            ("LA FIT", "GYM_MEMBERSHIP"),
            ("SPOTIFY SUBSCRIPTION", "SPOTIFY_MEMBERSHIP"),
            ("COMCAST CABLE", "COMCAST"),
            ("POWELL'S BURNSIDE", "POWELLS"),
            ("REGAL CINEMAS INC", "MOVIES"),
            ("MCDONALD'S", "FAST_FOOD"),
            ("NORDSTROM", "CLOTHES"),
            ("ARSENAL STORE", "ARSENAL"),
            ("EVERYDAY MUSIC", "PHYSICAL_MEDIA"),
            ("APPLE.COM/BILL", "APPLE_CLOUD_STORAGE"),
            ("AIRBNB * HMPSDMXX99", "TRAVEL_LODGING"),
            ("ALASKA AIR", "FLIGHTS"),
            ("PARKING LOT", "PARKING"),
            ("MODA CENTER", "MODA_CENTER"),
            ("ROKU FOR WARNERMEDIA GLOB", "HBO_SUBSCRIPTION"),
            ("PORTLAND INDOOR SOCCE", "INDOOR_SOCCER"),
            ("XBOX GAME PASS", "VIDEO_GAMES"),
            ("PLAYSTATION STORE", "VIDEO_GAMES"),
            ("WILLAMETTE DRY", "DRY_CLEANING"),
            ("PRIME VIDEO", "VOD_AMAZON"),
            ("GEICO INSURANCE", "CAR_INSURANCE"),
            ("LES SCHWAB TIRES #0243", "CAR_MAINTENANCE"),
            ("AMAZON WEB SERVICES", "HOSTING_SOFTWARE_PROJECTS"),
            ("CLAUDE.AI SUBSCRIPTION", "AI_SUBSCRIPTION"),
            ("AMAZON PRIME", "AMAZON_PRIME"),
            ("AMAZON.COM", "AMAZON_PURCHASE"),
            ("CHESS.COM", "CHESS_SUBSCRIPTION"),
            ("TCGPLAYER", "MTG"),
            ("ROKU FOR PEACOCK TV LLC", "PEACOCK_SUBSCRIPTION"),
            ("SHELL GAS", "GAS"),
            ("HRB ONLINE TAX PRODUCT", "FILING_TAXES"),
            ("JEWELERS-MUTUAL-PMNT", "DIAMOND_INSURANCE"),
            ("BLAZERVISION", "BLAZER_VISION_SUBSCRIPTION"),
            ("DUNCD ON PRIME", "PODCAST_SUBSCRIPTION"),
            ("ARTS TAX PAYMENT", "PORTLAND_ARTS_TAX"),
            ("OPAL CAMERA", "COMPUTERS_TECHNOLOGY_HARDWARE"),
            ("USPS PO 12345", "SHIPPING"),
            ("FEDEX OFFIC 67890", "SHIPPING"),
            ("RODEO BULL RIDING", "RODEO"),
            ("ENTERPRISE RENT A CAR", "OTHER_TRANSPORTATION"),
            ("RANDOM MERCHANT", "OTHER"),
        ],
    )
    def test_categorize_individual_credit_card_transaction(
        self, description, expected_category
    ):
        """Test credit card transaction categorization logic."""
        result = _categorize_individual_credit_card_transaction(description)
        assert result == expected_category

    def test_categorize_all_credit_cards_transactions_adds_category_column(self):
        """Test that categorization adds a category column to DataFrame."""
        df = pd.DataFrame(
            {"description": ["SQ *OVATION COFFEE", "SAFEWAY #2790", "ALASKA AIR"]}
        )

        result = _categorize_all_credit_cards_transactions(df)

        assert "category" in result.columns
        assert result["category"].iloc[0] == "OVATION"
        assert result["category"].iloc[1] == "GROCERIES"
        assert result["category"].iloc[2] == "FLIGHTS"

    def test_rename_chase_category_col(self):
        """Test that Chase category column is renamed."""
        df = pd.DataFrame({"category": ["Food & Drink", "Groceries"]})

        result = _rename_chase_category_col(df)

        assert "chase_category" in result.columns
        assert "category" not in result.columns

    def test_update_credit_card_transactions_categories_ovation_weekend(self):
        """Test that Ovation on weekends is categorized as OVATION_WEEKEND."""
        df = pd.DataFrame(
            {
                "category": ["OVATION", "OVATION", "OVATION"],
                "day_of_week": [6, 7, 1],  # Saturday, Sunday, Monday
                "chase_category": ["Food", "Food", "Food"],
            }
        )

        result = _update_credit_card_transactions_categories(df)

        assert result["category"].iloc[0] == "OVATION_WEEKEND"  # Saturday
        assert result["category"].iloc[1] == "OVATION_WEEKEND"  # Sunday
        assert result["category"].iloc[2] == "OVATION_WEEKDAY"  # Monday

    def test_update_credit_card_transactions_categories_other_to_eating_out(self):
        """Test that OTHER with Food & Drink Chase category becomes EATING_OUT."""
        df = pd.DataFrame(
            {
                "category": ["OTHER", "OTHER"],
                "day_of_week": [1, 2],
                "chase_category": ["Food & Drink", "Shopping"],
            }
        )

        result = _update_credit_card_transactions_categories(df)

        assert result["category"].iloc[0] == "EATING_OUT"
        assert result["category"].iloc[1] == "OTHER"


class TestStagingDataCleaningPipeline:
    """Test complete staging layer data cleaning pipeline."""

    @patch("etl.layers.staging.get_db")
    def test_clean_bank_account_data_applies_all_transformations(
        self, mock_get_db, temp_db_connection
    ):
        """Test that clean_bank_account_data applies all transformations."""
        # Create mock raw data
        mock_connection = MagicMock()
        mock_connection.connection.return_value = temp_db_connection
        mock_get_db.return_value = mock_connection

        raw_df = pd.DataFrame(
            {
                "id": ["test1"],
                "date": pd.to_datetime(["2024-01-15"]),
                "amount": [2500.00],
                "description": ["clearcover inc payroll"],  # lowercase
                "balance": [5000.00],
                "account": ["Chase1234"],
                "source_file": ["test.csv"],
                "imported_at": [pd.Timestamp.now()],
            }
        )

        # Write to temp database
        raw_df.to_sql(
            "raw_bank_account_transactions",
            temp_db_connection,
            if_exists="replace",
            index=False,
        )

        result = clean_bank_account_data()

        # Check transformations were applied
        assert "description" in result.columns
        assert result["description"].iloc[0] == "CLEARCOVER INC PAYROLL"  # Normalized
        assert "category" in result.columns
        assert result["category"].iloc[0] == "SALARY"  # Categorized
        assert "year" in result.columns
        assert "month" in result.columns
        assert "year_month" in result.columns

    @patch("etl.layers.staging.get_db")
    def test_clean_credit_card_data_applies_all_transformations(
        self, mock_get_db, temp_db_connection
    ):
        """Test that clean_credit_card_data applies all transformations."""
        mock_connection = MagicMock()
        mock_connection.connection.return_value = temp_db_connection
        mock_get_db.return_value = mock_connection

        raw_df = pd.DataFrame(
            {
                "id": ["test1"],
                "date": pd.to_datetime(["2024-01-15"]),
                "amount": [-12.50],
                "description": ["sq *ovation coffee"],  # lowercase
                "category": ["Food & Drink"],
                "card_number": ["Chase5678"],
                "source_file": ["test.csv"],
                "imported_at": [pd.Timestamp.now()],
            }
        )

        # Write to temp database
        raw_df.to_sql(
            "raw_credit_card_transactions",
            temp_db_connection,
            if_exists="replace",
            index=False,
        )

        result = clean_credit_card_data()

        # Check transformations were applied
        assert result["description"].iloc[0] == "SQ *OVATION COFFEE"  # Normalized
        assert "chase_category" in result.columns  # Renamed
        assert "category" in result.columns
        assert result["category"].iloc[0] in [
            "OVATION",
            "OVATION_WEEKDAY",
            "OVATION_WEEKEND",
        ]
        assert "year" in result.columns
        assert "month" in result.columns


class TestStagingTableCreation:
    """Test staging table creation functions."""

    @patch("etl.layers.staging.get_db")
    @patch("etl.layers.staging.clean_bank_account_data")
    def test_create_staging_bank_account_transactions_writes_to_db(
        self, mock_clean, mock_get_db, temp_db_connection
    ):
        """Test that staging table is created from cleaned data."""
        # Mock the clean function to return test data
        mock_clean.return_value = pd.DataFrame(
            {
                "id": ["test1"],
                "date": pd.to_datetime(["2024-01-15"]).date,
                "amount": [2500.00],
                "description": ["CLEARCOVER INC PAYROLL"],
                "balance": [5000.00],
                "account": ["Chase1234"],
                "category": ["SALARY"],
                "year": [2024],
                "month": [1],
                "year_month": ["2024-01"],
                "day_of_week": [1],
                "source_file": ["test.csv"],
                "imported_at": [pd.Timestamp.now()],
            }
        )

        # Mock database
        mock_db = MagicMock()
        mock_db.connection.return_value = temp_db_connection
        mock_get_db.return_value = mock_db

        # Call function
        create_staging_bank_account_transactions()

        # Verify table was created
        result = pd.read_sql(
            "SELECT * FROM staging_bank_account_transactions", temp_db_connection
        )
        assert len(result) == 1
        assert result["category"].iloc[0] == "SALARY"

    @patch("etl.layers.staging.get_db")
    @patch("etl.layers.staging.clean_credit_card_data")
    def test_create_staging_credit_card_transactions_writes_to_db(
        self, mock_clean, mock_get_db, temp_db_connection
    ):
        """Test that credit card staging table is created from cleaned data."""
        mock_clean.return_value = pd.DataFrame(
            {
                "id": ["test1"],
                "date": pd.to_datetime(["2024-01-15"]).date,
                "amount": [-12.50],
                "description": ["SQ *OVATION COFFEE"],
                "card_number": ["Chase5678"],
                "chase_category": ["Food & Drink"],
                "category": ["OVATION_WEEKDAY"],
                "year": [2024],
                "month": [1],
                "year_month": ["2024-01"],
                "day_of_week": [1],
                "source_file": ["test.csv"],
                "imported_at": [pd.Timestamp.now()],
            }
        )

        mock_db = MagicMock()
        mock_db.connection.return_value = temp_db_connection
        mock_get_db.return_value = mock_db

        create_staging_credit_card_transactions()

        result = pd.read_sql(
            "SELECT * FROM staging_credit_card_transactions", temp_db_connection
        )
        assert len(result) == 1
        assert result["category"].iloc[0] == "OVATION_WEEKDAY"
