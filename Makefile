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

.PHONY: backup
backup: ## Dump the warehouse to backups/ (raw layer is the only irreplaceable data)
	@mkdir -p backups
	docker compose exec -T postgres \
		pg_dump -U seismic -d seismic --format=custom --no-owner \
		> "backups/seismic-$$(date +%Y%m%dT%H%M%S).dump"
	@ls -1t backups | head -1 | sed 's/^/wrote backups\//'

.PHONY: restore
restore: ## Restore a dump, e.g. make restore FILE=backups/seismic-20260809T210000.dump
	@test -n "$(FILE)" || (echo "FILE= is required" && exit 1)
	docker compose exec -T postgres \
		pg_restore -U seismic -d seismic --clean --if-exists --no-owner < "$(FILE)"

.PHONY: logs
logs: ## Tail scheduler logs
	docker compose logs -f airflow-scheduler

.PHONY: monitor
monitor: ## Print the monitoring endpoints
	@echo "Grafana       http://localhost:3000  (admin / $${GRAFANA_ADMIN_PASSWORD:-admin})"
	@echo "Prometheus    http://localhost:9090  (alerts at /alerts)"
	@echo "Alertmanager  http://localhost:9093"
	@echo "Pushgateway   http://localhost:9091"
	@echo "Airflow       http://localhost:8080  (admin / admin)"

.PHONY: lint
lint: ## Run ruff
	ruff format --check .
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
