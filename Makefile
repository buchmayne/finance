.PHONY: up down build rebuild restart logs logs-api logs-dashboard clean help test test-verbose test-coverage test-etl test-api format lint type-check

.DEFAULT_GOAL := help

up: ## Start all services
	docker-compose up -d

down: ## Stop all services
	docker-compose down

build: ## Build containers (uses cache)
	docker-compose build

rebuild: ## Rebuild containers from scratch
	docker-compose build --no-cache

restart: down up ## Restart all services

logs: ## Tail logs from all services
	docker-compose logs -f

logs-api: ## Tail API logs only
	docker-compose logs -f api

shell-api: ## Open a shell in the API container
	docker-compose exec api /bin/bash


clean: ## Remove containers, volumes, and images
	docker-compose down -v --rmi all
	docker system prune -f

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

## Testing Commands

test: ## Run all tests
	uv run pytest

test-verbose: ## Run all tests with verbose output
	uv run pytest -v

test-coverage: ## Run tests with coverage report
	uv run pytest --cov=etl --cov=api --cov-report=html --cov-report=term

test-etl: ## Run only ETL tests
	uv run pytest tests/test_etl_*.py -v

test-api: ## Run only API tests
	uv run pytest tests/test_api_*.py -v

test-watch: ## Run tests in watch mode (requires pytest-watch)
	uv run ptw

## Development Commands

run-pipeline: ## Run the ETL pipeline
	uv run etl/pipeline.py

format: ## Format code with Black
	uv run black .

lint: ## Lint code (requires additional setup)
	uv run black --check .

type-check: ## Run mypy type checking
	uv run mypy .