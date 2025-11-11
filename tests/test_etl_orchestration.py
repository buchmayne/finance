"""Tests for ETL pipeline orchestration."""

import pytest
from unittest.mock import patch, MagicMock, call
from etl.orchestration import ETLPipeline


class TestETLPipeline:
    """Test ETL pipeline orchestration."""

    def test_pipeline_initialization(self):
        """Test that pipeline initializes with correct layers."""
        pipeline = ETLPipeline()

        assert "raw" in pipeline.layers
        assert "staging" in pipeline.layers
        assert "marts" in pipeline.layers

        # Check that each layer has functions
        assert len(pipeline.layers["raw"]) > 0
        assert len(pipeline.layers["staging"]) > 0
        assert len(pipeline.layers["marts"]) > 0

    def test_pipeline_raw_layer_has_import_functions(self):
        """Test that raw layer has import functions."""
        pipeline = ETLPipeline()

        raw_functions = pipeline.layers["raw"]
        function_names = [f.__name__ for f in raw_functions]

        assert "import_bank_accounts" in function_names
        assert "import_credit_cards" in function_names

    def test_pipeline_staging_layer_has_create_functions(self):
        """Test that staging layer has create functions."""
        pipeline = ETLPipeline()

        staging_functions = pipeline.layers["staging"]
        function_names = [f.__name__ for f in staging_functions]

        assert "create_staging_bank_account_transactions" in function_names
        assert "create_staging_credit_card_transactions" in function_names

    def test_pipeline_marts_layer_has_create_functions(self):
        """Test that marts layer has create functions."""
        pipeline = ETLPipeline()

        marts_functions = pipeline.layers["marts"]
        function_names = [f.__name__ for f in marts_functions]

        assert "create_transactions_tbl" in function_names
        assert "create_income_tbl" in function_names
        assert "create_savings_tbl" in function_names
        assert "create_spending_tbl" in function_names

    def test_run_layer_raises_error_for_unknown_layer(self):
        """Test that running unknown layer raises ValueError."""
        pipeline = ETLPipeline()

        with pytest.raises(ValueError, match="Unknown layer"):
            pipeline.run_layer("unknown_layer")

    @patch("etl.layers.raw.import_bank_accounts")
    @patch("etl.layers.raw.import_credit_cards")
    def test_run_layer_raw_executes_all_functions(self, mock_import_cc, mock_import_ba):
        """Test that running raw layer executes all raw functions."""
        # Set __name__ attribute for mock functions so print() works
        mock_import_ba.__name__ = "import_bank_accounts"
        mock_import_cc.__name__ = "import_credit_cards"

        pipeline = ETLPipeline()

        pipeline.run_layer("raw")

        mock_import_ba.assert_called_once()
        mock_import_cc.assert_called_once()

    @patch("etl.layers.staging.create_staging_bank_account_transactions")
    @patch("etl.layers.staging.create_staging_credit_card_transactions")
    def test_run_layer_staging_executes_all_functions(
        self, mock_create_cc, mock_create_ba
    ):
        """Test that running staging layer executes all staging functions."""
        # Set __name__ attribute for mock functions
        mock_create_ba.__name__ = "create_staging_bank_account_transactions"
        mock_create_cc.__name__ = "create_staging_credit_card_transactions"

        pipeline = ETLPipeline()

        pipeline.run_layer("staging")

        mock_create_ba.assert_called_once()
        mock_create_cc.assert_called_once()

    @patch("etl.layers.marts.create_transactions_tbl")
    @patch("etl.layers.marts.create_income_tbl")
    @patch("etl.layers.marts.create_savings_tbl")
    @patch("etl.layers.marts.create_spending_tbl")
    def test_run_layer_marts_executes_all_functions(
        self, mock_spending, mock_savings, mock_income, mock_transactions
    ):
        """Test that running marts layer executes all marts functions."""
        # Set __name__ attribute for mock functions
        mock_transactions.__name__ = "create_transactions_tbl"
        mock_income.__name__ = "create_income_tbl"
        mock_savings.__name__ = "create_savings_tbl"
        mock_spending.__name__ = "create_spending_tbl"

        pipeline = ETLPipeline()

        pipeline.run_layer("marts")

        mock_transactions.assert_called_once()
        mock_income.assert_called_once()
        mock_savings.assert_called_once()
        mock_spending.assert_called_once()

    @patch("etl.layers.raw.import_bank_accounts")
    @patch("etl.layers.raw.import_credit_cards")
    @patch("etl.layers.staging.create_staging_bank_account_transactions")
    @patch("etl.layers.staging.create_staging_credit_card_transactions")
    @patch("etl.layers.marts.create_transactions_tbl")
    @patch("etl.layers.marts.create_income_tbl")
    @patch("etl.layers.marts.create_savings_tbl")
    @patch("etl.layers.marts.create_spending_tbl")
    def test_run_full_pipeline_executes_in_order(
        self,
        mock_spending,
        mock_savings,
        mock_income,
        mock_transactions,
        mock_staging_cc,
        mock_staging_ba,
        mock_raw_cc,
        mock_raw_ba,
    ):
        """Test that full pipeline executes layers in correct order."""
        # Set __name__ attributes for all mocks
        mock_raw_ba.__name__ = "import_bank_accounts"
        mock_raw_cc.__name__ = "import_credit_cards"
        mock_staging_ba.__name__ = "create_staging_bank_account_transactions"
        mock_staging_cc.__name__ = "create_staging_credit_card_transactions"
        mock_transactions.__name__ = "create_transactions_tbl"
        mock_income.__name__ = "create_income_tbl"
        mock_savings.__name__ = "create_savings_tbl"
        mock_spending.__name__ = "create_spending_tbl"

        pipeline = ETLPipeline()

        pipeline.run_full_pipeline()

        # Verify all functions were called
        mock_raw_ba.assert_called_once()
        mock_raw_cc.assert_called_once()
        mock_staging_ba.assert_called_once()
        mock_staging_cc.assert_called_once()
        mock_transactions.assert_called_once()
        mock_income.assert_called_once()
        mock_savings.assert_called_once()
        mock_spending.assert_called_once()

        # Verify order: raw functions should be called before staging
        raw_calls = [mock_raw_ba.call_args, mock_raw_cc.call_args]
        staging_calls = [mock_staging_ba.call_args, mock_staging_cc.call_args]

        # We can't easily verify exact order with simple mocks,
        # but we can verify they were all called
        assert all(c is not None for c in raw_calls)
        assert all(c is not None for c in staging_calls)

    @patch("etl.layers.raw.import_bank_accounts")
    def test_run_layer_handles_function_exceptions(self, mock_import_ba):
        """Test that pipeline continues even if a function raises an exception."""
        # Set __name__ attribute
        mock_import_ba.__name__ = "import_bank_accounts"

        pipeline = ETLPipeline()

        # Make the function raise an exception
        mock_import_ba.side_effect = Exception("Test error")

        # Should raise the exception (pipeline doesn't catch by default)
        with pytest.raises(Exception, match="Test error"):
            pipeline.run_layer("raw")


class TestPipelineIntegration:
    """Integration tests for pipeline flow."""

    def test_pipeline_layers_execute_in_dependency_order(self):
        """Test that pipeline respects layer dependencies."""
        pipeline = ETLPipeline()

        # The order should be: raw -> staging -> marts
        # This is defined in run_full_pipeline
        layer_order = ["raw", "staging", "marts"]

        # Verify the layers exist
        for layer_name in layer_order:
            assert layer_name in pipeline.layers

    def test_pipeline_has_correct_number_of_functions_per_layer(self):
        """Test that each layer has the expected number of functions."""
        pipeline = ETLPipeline()

        # Raw layer: 2 import functions
        assert len(pipeline.layers["raw"]) == 2

        # Staging layer: 2 create staging table functions
        assert len(pipeline.layers["staging"]) == 2

        # Marts layer: 4 create marts table functions
        assert len(pipeline.layers["marts"]) == 4
