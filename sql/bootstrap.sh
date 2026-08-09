#!/bin/bash
# Creates the warehouse database alongside Airflow's metadata database, then
# applies the schema DDL. Runs once, on first initialisation of the volume.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
	CREATE USER seismic WITH PASSWORD '${SEISMIC_PASSWORD}';
	CREATE DATABASE seismic OWNER seismic;

	-- Grafana reads the warehouse through this role: SELECT and nothing else.
	CREATE USER seismic_readonly WITH PASSWORD '${READONLY_PASSWORD}';
	GRANT CONNECT ON DATABASE seismic TO seismic_readonly;

	-- postgres_exporter needs cluster-wide statistics, which pg_monitor grants
	-- without handing over any table data. Only a superuser can grant it, so it
	-- happens here rather than in the warehouse scripts below.
	CREATE USER seismic_exporter WITH PASSWORD '${EXPORTER_PASSWORD}';
	GRANT pg_monitor TO seismic_exporter;
	GRANT CONNECT ON DATABASE seismic TO seismic_exporter;
EOSQL

for script in /docker-entrypoint-initdb.d/warehouse/*.sql; do
	echo "applying ${script}"
	psql -v ON_ERROR_STOP=1 --username seismic --dbname seismic -f "$script"
done
