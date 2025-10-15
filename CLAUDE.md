# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a personal finance tracking and analysis system that ingests bank and credit card transaction data from CSV files, processes them through an ETL pipeline, and exposes analytics through a FastAPI backend with both Streamlit and vanilla JavaScript frontends.

## Architecture

### Data Flow (ETL Pipeline)

The system follows a three-layer ETL architecture inspired by modern data warehouse patterns:

1. **Raw Layer** (`etl/layers/raw.py`): Imports CSVs from `data/csv_files/` into SQLite tables
   - `BankAccountCSVImporter` and `CreditCardCSVImporter` classes handle Chase-specific formats
   - Generates unique transaction IDs using MD5 hashing (date + amount + description)
   - Extracts account/card numbers from file paths using regex patterns
   - Creates tables: `raw_bank_account_transactions`, `raw_credit_card_transactions`

2. **Staging Layer** (`etl/layers/staging.py`): Cleans and categorizes transactions
   - Normalizes descriptions (uppercase, whitespace cleanup)
   - Parses dates and adds temporal columns (year, month, day_of_week, year_month)
   - Applies custom categorization logic based on transaction descriptions
   - Creates tables: `staging_bank_account_transactions`, `staging_credit_card_transactions`

3. **Marts Layer** (`etl/layers/marts.py`): Creates analytics-ready tables
   - Unions bank and credit card transactions
   - Assigns meta_categories to group fine-grained categories
   - Separates data into domain tables:
     - `marts_transactions`: All transactions with meta categories (excludes savings)
     - `marts_spending`: Only spending (excludes income and savings)
     - `marts_income`: Only income transactions
     - `marts_savings`: Transfers to/from brokerage accounts

### Application Components

**API** (`api/`):
- FastAPI application serving metrics endpoints at `/api/metrics/*`
- Serves static frontend at `/control`
- Key endpoints support `period` (ytd, last_1/3/6/12_months, full_history) and `include_wedding` parameters
- Metrics module (`api/metrics.py`) contains reusable calculation functions

**Dashboard** (`dashboard/`):
- Streamlit application for interactive data visualization
- Fetches data from API endpoints
- Configurable time periods and wedding expense filtering

**Frontend** (`frontend/`):
- Simple HTML/CSS/JS interface served by the API
- Alternative to the Streamlit dashboard

**Database** (`etl/database.py`, `etl/config.py`):
- SQLite database (`finance.db`) for development
- Database URL configurable via environment variable `DATABASE_URL`

## Development Commands

### Running the ETL Pipeline
```bash
# Run the full pipeline (raw -> staging -> marts)
make run-pipeline
# or
uv run etl/pipeline.py

# Run individual layers programmatically
python -c "from etl.orchestration import ETLPipeline; ETLPipeline().run_layer('raw')"
```

### Docker Services
```bash
# Start all services (API on :8000, Dashboard on :8501)
make up

# View logs
make logs              # all services
make logs-api          # API only
make logs-dashboard    # dashboard only

# Stop services
make down

# Rebuild from scratch
make rebuild

# Clean everything
make clean
```

### Running Services Locally
```bash
# Run API directly
uv run uvicorn api.app:app --host 0.0.0.0 --port 8000 --reload

# Run Streamlit dashboard
uv run streamlit run dashboard/app.py
```

### Development Tools
```bash
# Format code with Black
uv run black .

# Type checking with mypy
uv run mypy .

# Run tests
uv run pytest
```

### Jupyter Notebooks
The `DEMO.ipynb` notebook contains exploratory analysis and demonstrations of the data pipeline.

## Key Implementation Details

### Transaction Categorization Logic

The staging layer applies extensive pattern matching to categorize transactions:
- Bank transactions: `_categorize_individual_bank_transaction()` in `etl/layers/staging.py:41`
- Credit card transactions: `_categorize_individual_credit_card_transaction()` in `etl/layers/staging.py:127`

Categories are then rolled up into meta_categories in the marts layer (`assign_categories_to_meta_categories()` at `etl/layers/marts.py:75`).

### Important Constants

- `INCOME_CATEGORIES`: Defined in `etl/layers/marts.py:7`
- `SAVINGS_CATEGORIES`: Defined in `etl/layers/marts.py:16`
- `MORTGAGE_CONTRIBUTION`: Hardcoded in `api/metrics.py:5` (used for budget calculations)

### Date Handling

The `year_month` column is formatted as "YYYY-MM" strings throughout the system and is the primary temporal key for aggregations.

### Data Sources

CSV files are expected in:
- `data/csv_files/` (contains subdirectories for bank accounts and credit cards)
- Paths configured via `PathCSVDirectories` enum in `etl/types.py`

## Testing

The project has a comprehensive test suite with 219 tests covering ETL and API functionality.

### Running Tests

```bash
# Run all tests
make test

# Run tests with verbose output
make test-verbose

# Run tests with coverage report
make test-coverage

# Run only ETL tests
make test-etl

# Run only API tests
make test-api

# Run specific test file
uv run pytest tests/test_etl_staging.py

# Run specific test class or function
uv run pytest tests/test_etl_staging.py::TestNormalizeDescription::test_normalize_description_converts_to_uppercase
```

### Test Organization

- `tests/conftest.py`: Shared fixtures (in-memory databases, sample DataFrames, temp CSV files)
- `tests/test_etl_raw.py`: CSV import and raw layer tests
- `tests/test_etl_staging.py`: Data cleaning and categorization tests (100+ parametrized cases)
- `tests/test_etl_marts.py`: Analytics table creation and filtering tests
- `tests/test_etl_orchestration.py`: Pipeline execution flow tests
- `tests/test_api_metrics.py`: Metrics calculation tests
- `tests/test_api_endpoints.py`: FastAPI endpoint tests

See `tests/README.md` for detailed testing documentation.

## Environment Configuration

Required environment variables:
- `DATABASE_URL`: SQLite connection string (default: `sqlite:///./finance.db`)
- `API_URL`: API base URL for dashboard (default: `http://localhost:8000`)

## Database Schema

The system uses SQLAlchemy models defined in `etl/models.py` for raw layer tables. Staging and marts tables are created directly via pandas `to_sql()` operations.

## Package Management

This project uses `uv` for dependency management:
- Dependencies defined in `pyproject.toml`
- Locked in `uv.lock`
- Dev dependencies include: black, mypy, pytest, jupyterlab

## Notes on Code Organization

- Pipeline orchestration: `etl/orchestration.py` defines layer execution order
- Column mappings for Chase CSV formats: `etl/mappings.py`
- Database connection factory: `etl/database.py:10` (`get_db()`)
- API metrics calculations are pure functions that take a database session and return pandas DataFrames
