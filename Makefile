.PHONY: up down build rebuild restart logs logs-api logs-dashboard clean help

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

logs-dashboard: ## Tail dashboard logs only
	docker-compose logs -f dashboard

shell-api: ## Open a shell in the API container
	docker-compose exec api /bin/bash

shell-dashboard: ## Open a shell in the dashboard container
	docker-compose exec dashboard /bin/bash

clean: ## Remove containers, volumes, and images
	docker-compose down -v --rmi all
	docker system prune -f

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

run-pipeline:
	uv run etl/pipeline.py