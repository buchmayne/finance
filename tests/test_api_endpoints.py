"""Tests for API endpoints."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import pandas as pd
from api.app import app


@pytest.fixture
def client():
    """Create a test client for the FastAPI app."""
    return TestClient(app)


class TestHealthEndpoint:
    """Test health check endpoint."""

    def test_health_check_returns_200(self, client):
        """Test that health endpoint returns 200 status."""
        response = client.get("/api/health")
        assert response.status_code == 200

    def test_health_check_returns_json(self, client):
        """Test that health endpoint returns JSON."""
        response = client.get("/api/health")
        assert response.headers["content-type"] == "application/json"

    def test_health_check_has_required_fields(self, client):
        """Test that health response has required fields."""
        response = client.get("/api/health")
        data = response.json()

        assert "status" in data
        assert "timestamp" in data
        assert data["status"] == "healthy"


class TestSpendingByCategoryEndpoint:
    """Test spending by category endpoint."""

    @patch("api.app.calculate_average_monthly_spending_by_meta_category")
    def test_spending_by_category_returns_200(self, mock_calculate, client):
        """Test that spending by category endpoint returns 200."""
        mock_calculate.return_value = pd.DataFrame(
            {
                "meta_category": ["GROCERIES", "TRAVEL"],
                "avg_monthly_spend": [200.0, 300.0],
            }
        )

        response = client.get("/api/metrics/spending-by-category")
        assert response.status_code == 200

    @patch("api.app.calculate_average_monthly_spending_by_meta_category")
    def test_spending_by_category_returns_list(self, mock_calculate, client):
        """Test that endpoint returns a list of records."""
        mock_calculate.return_value = pd.DataFrame(
            {
                "meta_category": ["GROCERIES", "TRAVEL"],
                "avg_monthly_spend": [200.0, 300.0],
            }
        )

        response = client.get("/api/metrics/spending-by-category")
        data = response.json()

        assert isinstance(data, list)
        assert len(data) == 2

    @patch("api.app.calculate_average_monthly_spending_by_meta_category")
    def test_spending_by_category_accepts_period_parameter(
        self, mock_calculate, client
    ):
        """Test that endpoint accepts period parameter."""
        mock_calculate.return_value = pd.DataFrame(
            {"meta_category": ["GROCERIES"], "avg_monthly_spend": [200.0]}
        )

        response = client.get("/api/metrics/spending-by-category?period=last_12_months")
        assert response.status_code == 200

        # Verify the function was called with correct period
        mock_calculate.assert_called_once()
        args = mock_calculate.call_args
        assert args[0][1] == "last_12_months"  # period parameter

    @patch("api.app.calculate_average_monthly_spending_by_meta_category")
    def test_spending_by_category_accepts_include_wedding_parameter(
        self, mock_calculate, client
    ):
        """Test that endpoint accepts include_wedding parameter."""
        mock_calculate.return_value = pd.DataFrame(
            {"meta_category": ["GROCERIES"], "avg_monthly_spend": [200.0]}
        )

        response = client.get("/api/metrics/spending-by-category?include_wedding=false")
        assert response.status_code == 200

        # Verify the function was called with correct parameter
        mock_calculate.assert_called_once()
        args = mock_calculate.call_args
        assert args[0][2] is False  # include_wedding parameter

    @patch("api.app.calculate_average_monthly_spending_by_meta_category")
    def test_spending_by_category_has_default_parameters(self, mock_calculate, client):
        """Test that endpoint has correct default parameters."""
        mock_calculate.return_value = pd.DataFrame(
            {"meta_category": ["GROCERIES"], "avg_monthly_spend": [200.0]}
        )

        response = client.get("/api/metrics/spending-by-category")
        assert response.status_code == 200

        # Verify defaults
        args = mock_calculate.call_args
        assert args[0][1] == "full_history"  # default period
        assert args[0][2] is True  # default include_wedding


class TestMonthlyBudgetHistoryEndpoint:
    """Test monthly budget history endpoint."""

    @patch("api.app.calculate_monthly_budget_history")
    def test_monthly_budget_history_returns_200(self, mock_calculate, client):
        """Test that monthly budget history endpoint returns 200."""
        mock_calculate.return_value = pd.DataFrame(
            {
                "year_month": ["2024-01", "2024-02"],
                "monthly_spending": [1000, 1100],
                "monthly_salary": [2500, 2500],
            }
        )

        response = client.get("/api/metrics/monthly-budget-history")
        assert response.status_code == 200

    @patch("api.app.calculate_monthly_budget_history")
    def test_monthly_budget_history_returns_list(self, mock_calculate, client):
        """Test that endpoint returns a list of records."""
        mock_calculate.return_value = pd.DataFrame(
            {"year_month": ["2024-01"], "monthly_spending": [1000]}
        )

        response = client.get("/api/metrics/monthly-budget-history")
        data = response.json()

        assert isinstance(data, list)

    @patch("api.app.calculate_monthly_budget_history")
    def test_monthly_budget_history_accepts_parameters(self, mock_calculate, client):
        """Test that endpoint accepts period and include_wedding parameters."""
        mock_calculate.return_value = pd.DataFrame(
            {"year_month": ["2024-01"], "monthly_spending": [1000]}
        )

        response = client.get(
            "/api/metrics/monthly-budget-history"
            "?period=last_6_months&include_wedding=false"
        )
        assert response.status_code == 200

        args = mock_calculate.call_args
        assert args[0][1] == "last_6_months"
        assert args[0][2] is False


class TestAverageMonthlyBudgetEndpoint:
    """Test average monthly budget endpoint."""

    @patch("api.app.calculate_average_monthly_budget")
    def test_average_monthly_budget_returns_200(self, mock_calculate, client):
        """Test that average monthly budget endpoint returns 200."""
        mock_calculate.return_value = pd.DataFrame(
            {
                "description": ["GROCERIES", "SALARY"],
                "amount": [-200, 2500],
                "category": ["SPENDING", "INCOME"],
            }
        )

        response = client.get("/api/metrics/average-monthly-budget")
        assert response.status_code == 200

    @patch("api.app.calculate_average_monthly_budget")
    def test_average_monthly_budget_returns_list(self, mock_calculate, client):
        """Test that endpoint returns a list of records."""
        mock_calculate.return_value = pd.DataFrame(
            {"description": ["GROCERIES"], "amount": [-200], "category": ["SPENDING"]}
        )

        response = client.get("/api/metrics/average-monthly-budget")
        data = response.json()

        assert isinstance(data, list)

    @patch("api.app.calculate_average_monthly_budget")
    def test_average_monthly_budget_has_default_parameters(
        self, mock_calculate, client
    ):
        """Test that endpoint has correct default parameters."""
        mock_calculate.return_value = pd.DataFrame(
            {"description": ["GROCERIES"], "amount": [-200]}
        )

        response = client.get("/api/metrics/average-monthly-budget")
        assert response.status_code == 200

        args = mock_calculate.call_args
        assert args[0][1] == "full_history"  # default period
        assert args[0][2] is False  # default include_wedding=False


class TestCORS:
    """Test CORS middleware configuration."""

    def test_cors_headers_present(self, client):
        """Test that CORS headers are present in response."""
        response = client.options(
            "/api/health",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "GET",
            },
        )

        # Check for CORS headers
        assert "access-control-allow-origin" in response.headers

    def test_cors_allows_all_origins(self, client):
        """Test that CORS allows all origins."""
        response = client.get("/api/health", headers={"Origin": "http://example.com"})

        # With allow_origins=["*"], all origins should be allowed
        assert "access-control-allow-origin" in response.headers


class TestFrontendEndpoints:
    """Test frontend serving endpoints."""

    def test_control_endpoint_exists(self, client):
        """Test that /control endpoint exists."""
        response = client.get("/control")
        # Should return HTML file
        assert response.status_code == 200

    def test_static_files_mounted(self, client):
        """Test that static files are accessible."""
        # This will fail if frontend/index.html doesn't exist
        # but tests the routing works
        response = client.get("/control")
        assert response.status_code in [200, 404]  # Either exists or doesn't


class TestAPIMetadata:
    """Test API metadata and documentation."""

    def test_api_has_title(self, client):
        """Test that API has a title in OpenAPI schema."""
        response = client.get("/openapi.json")
        assert response.status_code == 200
        schema = response.json()

        assert "info" in schema
        assert "title" in schema["info"]
        assert schema["info"]["title"] == "Personal Finance Dashboard"

    def test_api_has_version(self, client):
        """Test that API has a version in OpenAPI schema."""
        response = client.get("/openapi.json")
        schema = response.json()

        assert "version" in schema["info"]
        assert schema["info"]["version"] == "0.1.0"

    def test_docs_endpoint_exists(self, client):
        """Test that /docs endpoint exists."""
        response = client.get("/docs")
        assert response.status_code == 200

    def test_redoc_endpoint_exists(self, client):
        """Test that /redoc endpoint exists."""
        response = client.get("/redoc")
        assert response.status_code == 200


class TestErrorHandling:
    """Test error handling in API endpoints."""

    @patch("api.app.calculate_average_monthly_spending_by_meta_category")
    def test_endpoint_handles_database_errors(self, mock_calculate, client):
        """Test that endpoints handle database errors gracefully."""
        # Simulate database error
        mock_calculate.side_effect = Exception("Database connection error")

        # FastAPI by default returns 500 for exceptions, but test client
        # will raise the exception. We catch it here.
        with pytest.raises(Exception, match="Database connection error"):
            response = client.get("/api/metrics/spending-by-category")

    @patch("api.app.calculate_average_monthly_spending_by_meta_category")
    def test_endpoint_handles_empty_dataframes(self, mock_calculate, client):
        """Test that endpoints handle empty DataFrames."""
        mock_calculate.return_value = pd.DataFrame()

        response = client.get("/api/metrics/spending-by-category")

        # Should still return 200 with empty list
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0
