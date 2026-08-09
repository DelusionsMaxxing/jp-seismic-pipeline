-- Bootstrap schema, applied by the Postgres image on first start.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Landing table. The GeoJSON feature is stored whole so that a schema change
-- upstream never loses data: parsing happens downstream in dbt, where it is
-- version-controlled and covered by tests.
CREATE TABLE IF NOT EXISTS raw.earthquake_events (
    event_id    text        PRIMARY KEY,
    payload     jsonb       NOT NULL,
    source      text        NOT NULL DEFAULT 'usgs',
    ingested_at timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_earthquake_events_updated_at
    ON raw.earthquake_events (updated_at);

-- Event time lives inside the payload as epoch milliseconds; indexing the
-- expression keeps incremental dbt runs from sequential-scanning the table.
CREATE INDEX IF NOT EXISTS idx_earthquake_events_event_time
    ON raw.earthquake_events (((payload -> 'properties' ->> 'time')::bigint));
