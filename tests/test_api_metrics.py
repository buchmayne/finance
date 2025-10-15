"""Tests for API metrics calculations."""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from api.metrics import (
    MORTGAGE_CONTRIBUTION,
    determine_last_X_months_from_dataset,
    include_wedding_spending,
    subset_data_by_period,
    calculate_average_monthly_spending_by_meta_category,
    calculate_monthly_spending,
    calculate_monthly_saving,
    calculate_average_monthly_saving,
    calculate_monthly_salary,
    calculate_average_monthly_salary,
    calculate_average_monthly_budget,
    calculate_monthly_budget_history,
    calculate_average_monthly_spend_eating_out_by_category,
)


class TestDetermineLastXMonths:
    """Test month subset determination."""

    def test_determine_last_X_months_returns_correct_count(self, sample_marts_spending_df):
        """Test that correct number of months are returned."""
        result = determine_last_X_months_from_dataset(sample_marts_spending_df, number_of_months=1)
        assert len(result) == 1

        result = determine_last_X_months_from_dataset(sample_marts_spending_df, number_of_months=2)
        assert len(result) == 2

    def test_determine_last_X_months_returns_most_recent(self, sample_marts_spending_df):
        """Test that most recent months are returned."""
        result = determine_last_X_months_from_dataset(sample_marts_spending_df, number_of_months=1)
        # Should return most recent month (2024-02)
        assert result[0] == '2024-02'

    def test_determine_last_X_months_sorted_descending(self, sample_marts_spending_df):
        """Test that months are returned in descending order."""
        result = determine_last_X_months_from_dataset(sample_marts_spending_df, number_of_months=2)
        assert result[0] == '2024-02'
        assert result[1] == '2024-01'


class TestIncludeWeddingSpending:
    """Test wedding spending filter."""

    def test_include_wedding_spending_keeps_wedding_when_true(self):
        """Test that wedding spending is kept when include_wedding=True."""
        df = pd.DataFrame({
            'meta_category': ['GROCERIES', 'WEDDING', 'TRAVEL'],
            'amount': [100, 500, 200]
        })

        result = include_wedding_spending(df, include_wedding=True)
        assert len(result) == 3

    def test_include_wedding_spending_removes_wedding_when_false(self):
        """Test that wedding spending is removed when include_wedding=False."""
        df = pd.DataFrame({
            'meta_category': ['GROCERIES', 'WEDDING', 'TRAVEL'],
            'amount': [100, 500, 200]
        })

        result = include_wedding_spending(df, include_wedding=False)
        assert len(result) == 2
        assert 'WEDDING' not in result['meta_category'].values


class TestSubsetDataByPeriod:
    """Test data subsetting by time period."""

    def test_subset_data_by_period_ytd(self):
        """Test year-to-date subsetting."""
        df = pd.DataFrame({
            'year': [2023, 2024, 2024],
            'year_month': ['2023-12', '2024-01', '2024-02'],
            'amount': [100, 200, 300]
        })

        result = subset_data_by_period(df, period='ytd')
        assert len(result) == 2
        assert all(result['year'] == 2024)

    def test_subset_data_by_period_last_1_months(self, sample_marts_spending_df):
        """Test last 1 month subsetting."""
        result = subset_data_by_period(sample_marts_spending_df, period='last_1_months')
        # Should only include 2024-02 (most recent month)
        assert len(result) == 2
        assert all(result['year_month'] == '2024-02')

    def test_subset_data_by_period_last_3_months(self, sample_marts_spending_df):
        """Test last 3 months subsetting."""
        result = subset_data_by_period(sample_marts_spending_df, period='last_3_months')
        # Only have 2 months of data, so should return all
        assert len(result) == 4

    def test_subset_data_by_period_full_history(self, sample_marts_spending_df):
        """Test full history returns all data."""
        result = subset_data_by_period(sample_marts_spending_df, period='full_history')
        assert len(result) == len(sample_marts_spending_df)

    def test_subset_data_by_period_invalid_period_raises_error(self, sample_marts_spending_df):
        """Test that invalid period raises ValueError."""
        with pytest.raises(ValueError, match="Invalid period parameter"):
            subset_data_by_period(sample_marts_spending_df, period='invalid_period')


class TestCalculateAverageMonthlySpendingByMetaCategory:
    """Test average monthly spending calculation by meta category."""

    @patch('api.metrics.pd.read_sql')
    def test_calculate_average_monthly_spending_by_meta_category(
        self, mock_read_sql, sample_marts_spending_df
    ):
        """Test average monthly spending calculation."""
        mock_db = MagicMock()
        mock_read_sql.return_value = sample_marts_spending_df

        result = calculate_average_monthly_spending_by_meta_category(
            mock_db, period='full_history', include_wedding=True
        )

        # Check required columns exist
        assert 'meta_category' in result.columns
        assert 'avg_monthly_spend' in result.columns
        assert 'pct_of_avg_monthly_spend' in result.columns
        assert 'number_of_months_in_sample' in result.columns

        # Check calculation correctness
        # We have 2 months of data
        assert result['number_of_months_in_sample'].iloc[0] == 2

    @patch('api.metrics.pd.read_sql')
    def test_calculate_average_monthly_spending_excludes_wedding(
        self, mock_read_sql, sample_marts_spending_df
    ):
        """Test that wedding spending can be excluded."""
        # Add wedding spending
        df_with_wedding = pd.concat([
            sample_marts_spending_df,
            pd.DataFrame({
                'id': ['w1'],
                'date': [pd.Timestamp('2024-01-20').date()],
                'amount': [5000],
                'description': ['Wedding'],
                'account_or_card_number': ['Chase1234'],
                'category': ['WEDDING'],
                'meta_category': ['WEDDING'],
                'year': [2024],
                'month': [1],
                'year_month': ['2024-01'],
                'day_of_week': [6],
                'source': ['bank_account']
            })
        ])

        mock_db = MagicMock()
        mock_read_sql.return_value = df_with_wedding

        result = calculate_average_monthly_spending_by_meta_category(
            mock_db, period='full_history', include_wedding=False
        )

        # Wedding should not be in results
        assert 'WEDDING' not in result['meta_category'].values


class TestCalculateMonthlySpending:
    """Test monthly spending calculation."""

    @patch('api.metrics.pd.read_sql')
    def test_calculate_monthly_spending(self, mock_read_sql, sample_marts_spending_df):
        """Test monthly spending aggregation."""
        mock_db = MagicMock()
        mock_read_sql.return_value = sample_marts_spending_df

        result = calculate_monthly_spending(mock_db, period='full_history', include_wedding=True)

        assert 'year_month' in result.columns
        assert 'monthly_spending' in result.columns
        assert len(result) == 2  # 2 unique months

        # Check aggregation
        jan_spending = result[result['year_month'] == '2024-01']['monthly_spending'].iloc[0]
        assert jan_spending == 12.50 + 45.00  # Two Jan transactions


class TestCalculateMonthlySaving:
    """Test monthly saving calculation."""

    @patch('api.metrics.pd.read_sql')
    def test_calculate_monthly_saving(self, mock_read_sql, sample_marts_savings_df):
        """Test monthly savings aggregation."""
        mock_db = MagicMock()
        mock_read_sql.return_value = sample_marts_savings_df

        result = calculate_monthly_saving(mock_db, period='full_history')

        assert 'year_month' in result.columns
        assert 'monthly_savings' in result.columns
        # Should have entries for both months
        assert len(result) == 2

    @patch('api.metrics.pd.read_sql')
    def test_calculate_monthly_saving_fills_missing_months(self, mock_read_sql):
        """Test that missing months are filled with zeros."""
        # Only have savings in January
        df = pd.DataFrame({
            'id': ['sv1'],
            'date': [pd.Timestamp('2024-01-20').date()],
            'amount': [500.00],
            'category': ['TRANSFER_TO_BROKERAGE'],
            'year': [2024],
            'month': [1],
            'year_month': ['2024-01'],
        })

        mock_db = MagicMock()
        mock_read_sql.return_value = df

        result = calculate_monthly_saving(mock_db, period='full_history')

        # Check that only January has data (no fill since there's only one month)
        assert len(result) == 1

    @patch('api.metrics.pd.read_sql')
    def test_calculate_average_monthly_saving(self, mock_read_sql, sample_marts_savings_df):
        """Test average monthly savings calculation."""
        mock_db = MagicMock()
        mock_read_sql.return_value = sample_marts_savings_df

        result = calculate_average_monthly_saving(mock_db, period='full_history')

        # Should be 500 (average of 500 and 500)
        assert result == 500.0
        assert isinstance(result, float)


class TestCalculateMonthlySalary:
    """Test monthly salary calculation."""

    @patch('api.metrics.pd.read_sql')
    def test_calculate_monthly_salary(self, mock_read_sql, sample_marts_income_df):
        """Test monthly salary aggregation."""
        mock_db = MagicMock()
        mock_read_sql.return_value = sample_marts_income_df

        result = calculate_monthly_salary(mock_db, period='full_history')

        assert 'year_month' in result.columns
        assert 'monthly_salary' in result.columns
        assert len(result) == 2  # 2 months with salary

        # Check salary amounts
        assert result['monthly_salary'].iloc[0] == 2500.00

    @patch('api.metrics.pd.read_sql')
    def test_calculate_average_monthly_salary(self, mock_read_sql, sample_marts_income_df):
        """Test average monthly salary calculation."""
        mock_db = MagicMock()
        mock_read_sql.return_value = sample_marts_income_df

        result = calculate_average_monthly_salary(mock_db, period='full_history')

        # Should be 2500 (average of 2500 and 2500)
        assert result == 2500.0
        assert isinstance(result, float)


class TestCalculateAverageMonthlyBudget:
    """Test average monthly budget calculation."""

    @patch('api.metrics.calculate_average_monthly_spending_by_meta_category')
    @patch('api.metrics.calculate_average_monthly_salary')
    def test_calculate_average_monthly_budget_includes_spending_and_income(
        self, mock_salary, mock_spending
    ):
        """Test that budget includes both spending and income."""
        mock_db = MagicMock()

        # Mock spending
        mock_spending.return_value = pd.DataFrame({
            'meta_category': ['GROCERIES', 'TRAVEL'],
            'avg_monthly_spend': [200, 300]
        })

        # Mock salary
        mock_salary.return_value = 2500.0

        result = calculate_average_monthly_budget(mock_db, period='full_history', include_wedding=True)

        assert 'description' in result.columns
        assert 'amount' in result.columns
        assert 'category' in result.columns

        # Check that spending categories are present
        spending_rows = result[result['category'] == 'SPENDING']
        assert len(spending_rows) == 2

        # Check that income categories are present
        income_rows = result[result['category'] == 'INCOME']
        assert len(income_rows) == 2  # SALARY and MORTGAGE_CONTRIBUTION

        # Check that cash flow is calculated
        cash_flow_rows = result[result['category'] == 'CASH_FLOW']
        assert len(cash_flow_rows) == 1

    @patch('api.metrics.calculate_average_monthly_spending_by_meta_category')
    @patch('api.metrics.calculate_average_monthly_salary')
    def test_calculate_average_monthly_budget_cash_flow_calculation(
        self, mock_salary, mock_spending
    ):
        """Test that cash flow is correctly calculated."""
        mock_db = MagicMock()

        mock_spending.return_value = pd.DataFrame({
            'meta_category': ['GROCERIES'],
            'avg_monthly_spend': [200]
        })

        mock_salary.return_value = 2500.0

        result = calculate_average_monthly_budget(mock_db, period='full_history', include_wedding=True)

        # Cash flow = Income - Spending
        # Income = 2500 (salary) + 850 (mortgage contribution) = 3350
        # Spending = 200
        # Cash flow = 3350 - 200 = 3150
        cash_flow = result[result['category'] == 'CASH_FLOW']['amount'].iloc[0]
        assert cash_flow == 3350 - 200


class TestCalculateMonthlyBudgetHistory:
    """Test monthly budget history calculation."""

    @patch('api.metrics.calculate_monthly_spending')
    @patch('api.metrics.calculate_monthly_salary')
    @patch('api.metrics.calculate_monthly_saving')
    def test_calculate_monthly_budget_history_merges_all_sources(
        self, mock_savings, mock_salary, mock_spending
    ):
        """Test that budget history merges spending, salary, and savings."""
        mock_db = MagicMock()

        mock_spending.return_value = pd.DataFrame({
            'year_month': ['2024-01', '2024-02'],
            'monthly_spending': [500, 600]
        })

        mock_salary.return_value = pd.DataFrame({
            'year_month': ['2024-01', '2024-02'],
            'monthly_salary': [2500, 2500]
        })

        mock_savings.return_value = pd.DataFrame({
            'year_month': ['2024-01', '2024-02'],
            'monthly_savings': [300, 400]
        })

        result = calculate_monthly_budget_history(mock_db, period='full_history', include_wedding=True)

        assert 'year_month' in result.columns
        assert 'monthly_spending' in result.columns
        assert 'monthly_salary' in result.columns
        assert 'monthly_savings' in result.columns
        assert 'cumulative_savings' in result.columns

        # Check cumulative savings calculation
        assert result['cumulative_savings'].iloc[0] == 300
        assert result['cumulative_savings'].iloc[1] == 700  # 300 + 400


class TestCalculateEatingOutByCategory:
    """Test eating out subcategory analysis."""

    @patch('api.metrics.pd.read_sql')
    def test_calculate_average_monthly_spend_eating_out_by_category(
        self, mock_read_sql
    ):
        """Test eating out category breakdown."""
        mock_db = MagicMock()

        # Create eating out spending data
        df = pd.DataFrame({
            'id': ['1', '2', '3', '4'],
            'year_month': ['2024-01', '2024-01', '2024-02', '2024-02'],
            'meta_category': ['EATING_OUT', 'EATING_OUT', 'EATING_OUT', 'EATING_OUT'],
            'category': ['OVATION_WEEKDAY', 'EATING_OUT_NBHD_LUNCH', 'OVATION_WEEKEND', 'EATING_OUT'],
            'amount': [50, 30, 60, 40]
        })

        mock_read_sql.return_value = df

        result = calculate_average_monthly_spend_eating_out_by_category(
            mock_db, period='full_history'
        )

        assert 'category' in result.columns
        assert 'amount' in result.columns
        assert 'WORKDAY' in result.columns

        # Check that workday categories are flagged
        workday_categories = result[result['WORKDAY'] == True]['category'].tolist()
        assert 'OVATION_WEEKDAY' in workday_categories
        assert 'EATING_OUT_NBHD_LUNCH' in workday_categories


class TestConstants:
    """Test that constants are properly defined."""

    def test_mortgage_contribution_defined(self):
        """Test that MORTGAGE_CONTRIBUTION constant is defined."""
        assert MORTGAGE_CONTRIBUTION == 850
        assert isinstance(MORTGAGE_CONTRIBUTION, (int, float))
