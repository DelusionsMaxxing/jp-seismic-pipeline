-- Read-only access for the monitoring stack. Runs as the warehouse owner,
-- after bootstrap.sh has created the roles.

-- dbt would create these itself on first run, but Grafana's role needs USAGE
-- on them before that happens — otherwise the dashboard's panels error until
-- somebody remembers to re-grant after every new schema appears.
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS analytics_staging;
CREATE SCHEMA IF NOT EXISTS analytics_intermediate;
CREATE SCHEMA IF NOT EXISTS analytics_marts;

GRANT USAGE ON SCHEMA
    raw,
    analytics,
    analytics_staging,
    analytics_intermediate,
    analytics_marts
    TO seismic_readonly;

GRANT SELECT ON ALL TABLES IN SCHEMA
    raw,
    analytics,
    analytics_staging,
    analytics_intermediate,
    analytics_marts
    TO seismic_readonly;

-- dbt drops and recreates its models on every build, so grants on the tables
-- that exist today are worthless by tomorrow. Default privileges cover the
-- ones the pipeline has not created yet.
ALTER DEFAULT PRIVILEGES FOR ROLE seismic IN SCHEMA
    raw,
    analytics,
    analytics_staging,
    analytics_intermediate,
    analytics_marts
    GRANT SELECT ON TABLES TO seismic_readonly;
