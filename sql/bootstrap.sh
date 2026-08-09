#!/bin/bash
# Creates the warehouse database alongside Airflow's metadata database, then
# applies the schema DDL. Runs once, on first initialisation of the volume.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
	CREATE USER seismic WITH PASSWORD 'seismic';
	CREATE DATABASE seismic OWNER seismic;
EOSQL

for script in /docker-entrypoint-initdb.d/warehouse/*.sql; do
	echo "applying ${script}"
	psql -v ON_ERROR_STOP=1 --username seismic --dbname seismic -f "$script"
done
