.DEFAULT_GOAL := help
SHELL := /bin/bash

DBT := cd dbt && dbt

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: install
install: ## Install the package with dev extras
	pip install -e ".[dev]"
	$(DBT) deps

.PHONY: up
up: ## Start Postgres, the Airflow scheduler and the webserver
	docker compose up -d --build

.PHONY: down
down: ## Stop the stack, keeping data volumes
	docker compose down

.PHONY: clean
clean: ## Stop the stack and delete data volumes
	docker compose down -v

.PHONY: logs
logs: ## Tail scheduler logs
	docker compose logs -f airflow-scheduler

.PHONY: lint
lint: ## Run ruff
	ruff check src tests dags

.PHONY: test
test: ## Run unit tests
	pytest

.PHONY: ingest
ingest: ## Ingest yesterday's events into the raw layer
	python -m jp_seismic.cli

.PHONY: backfill
backfill: ## Backfill a range, e.g. make backfill START=2024-01-01 END=2024-04-01
	python -m jp_seismic.cli --start $(START) --end $(END)

.PHONY: demo
demo: ## Load fixture events instead of calling the API
	docker compose exec -T postgres \
		psql -U seismic -d seismic -v ON_ERROR_STOP=1 < sql/fixtures/sample_events.sql

.PHONY: build
build: ## Run every dbt model and its tests
	$(DBT) build --target dev

.PHONY: docs
docs: ## Generate and serve the dbt documentation site
	$(DBT) docs generate --target dev && $(DBT) docs serve
