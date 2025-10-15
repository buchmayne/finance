# Tests

This directory contains the test suite for the finance ETL and API project.

## Test Structure

```
tests/
├── conftest.py                      # Shared fixtures and test configuration
├── fixtures/                        # Test data fixtures
├── test_etl_raw.py                 # Tests for raw layer (CSV import)
├── test_etl_staging.py             # Tests for staging layer (cleaning/categorization)
├── test_etl_marts.py               # Tests for marts layer (analytics tables)
├── test_etl_orchestration.py      # Tests for pipeline orchestration
├── test_api_metrics.py             # Tests for metrics calculations
└── test_api_endpoints.py           # Tests for FastAPI endpoints
```

## Running Tests

### Run all tests
```bash
uv run pytest
```

### Run specific test file
```bash
uv run pytest tests/test_etl_raw.py
```

### Run specific test class
```bash
uv run pytest tests/test_etl_raw.py::TestCSVImporter
```

### Run specific test
```bash
uv run pytest tests/test_etl_raw.py::TestCSVImporter::test_generate_id_creates_consistent_hash
```

### Run tests with verbose output
```bash
uv run pytest -v
```

### Run tests with coverage (requires pytest-cov)
```bash
uv run pytest --cov=etl --cov=api --cov-report=html
```

### Run tests by marker
```bash
# Run only unit tests
uv run pytest -m unit

# Run only integration tests
uv run pytest -m integration

# Run only ETL tests
uv run pytest -m etl

# Run only API tests
uv run pytest -m api

# Skip slow tests
uv run pytest -m "not slow"
```

## Test Categories

### Unit Tests
- **Raw Layer** (`test_etl_raw.py`): CSV import, data cleaning, file processing
- **Staging Layer** (`test_etl_staging.py`): Normalization, categorization logic
- **Marts Layer** (`test_etl_marts.py`): Transaction filtering, meta-category assignment
- **Orchestration** (`test_etl_orchestration.py`): Pipeline execution flow
- **API Metrics** (`test_api_metrics.py`): Calculation functions
- **API Endpoints** (`test_api_endpoints.py`): FastAPI routes and responses

### Key Test Patterns

#### Fixtures
Shared test data and database connections are defined in `conftest.py`:
- `temp_db`: In-memory SQLite database with SQLAlchemy session
- `temp_db_connection`: Raw database connection for pandas operations
- `sample_*_df`: Pre-populated DataFrames for testing
- `sample_*_csv`: Temporary CSV files for import testing

#### Mocking
Tests use `unittest.mock.patch` to isolate units:
```python
@patch('etl.layers.staging.get_db')
def test_something(mock_get_db):
    mock_get_db.return_value = MagicMock()
    # Test code
```

#### Parametrized Tests
Common pattern for testing multiple inputs:
```python
@pytest.mark.parametrize("description,expected_category", [
    ('SAFEWAY', 'GROCERIES'),
    ('ALASKA AIR', 'FLIGHTS'),
])
def test_categorization(description, expected_category):
    assert categorize(description) == expected_category
```

## Writing New Tests

### Best Practices
1. **One assertion per test** when possible
2. **Use descriptive test names** that explain what is being tested
3. **Arrange-Act-Assert** pattern:
   ```python
   def test_something():
       # Arrange: Set up test data
       df = pd.DataFrame(...)

       # Act: Execute the code being tested
       result = function_under_test(df)

       # Assert: Verify the results
       assert result == expected_value
   ```
4. **Use fixtures** for common setup
5. **Mock external dependencies** (database, file system, etc.)
6. **Test edge cases** (empty data, missing values, etc.)

### Example Test
```python
def test_normalize_description_converts_to_uppercase():
    """Test that descriptions are converted to uppercase."""
    # Arrange
    df = pd.DataFrame({
        'description': ['test transaction']
    })

    # Act
    result = _normalize_description(df)

    # Assert
    assert result['description'].iloc[0] == 'TEST TRANSACTION'
```

## Test Coverage

Current test coverage includes:
- ✅ CSV import (raw layer)
- ✅ Data normalization (staging layer)
- ✅ Transaction categorization (staging layer)
- ✅ Meta-category assignment (marts layer)
- ✅ Transaction filtering (marts layer)
- ✅ Pipeline orchestration
- ✅ Metrics calculations
- ✅ API endpoints

## Continuous Integration

Tests should be run in CI/CD pipeline before deployment:
```yaml
# Example GitHub Actions workflow
- name: Run tests
  run: |
    uv run pytest --cov=etl --cov=api
```

## Troubleshooting

### Import Errors
If you see import errors, ensure you've installed the package:
```bash
uv sync
```

### Database Errors
Tests use in-memory SQLite databases. If you see database-related errors:
1. Check that fixtures are properly creating/cleaning up databases
2. Ensure `temp_db` fixture is being used in function signature

### Fixture Not Found
If pytest can't find a fixture:
1. Check that `conftest.py` is in the tests directory
2. Ensure the fixture is defined and not commented out
3. Check fixture scope (function, module, session)

## Adding Test Dependencies

To add testing libraries:
```bash
# Add to dev dependency group
uv add --dev pytest-cov  # For coverage reports
uv add --dev pytest-mock  # For easier mocking
uv add --dev pytest-xdist  # For parallel test execution
```
