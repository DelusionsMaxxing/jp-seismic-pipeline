-- Flattens the raw GeoJSON feature into typed columns. No business logic
-- here: this model only casts and renames, so a change in USGS's payload
-- shape has exactly one place to be fixed.

with source as (

    select * from {{ source('raw', 'earthquake_events') }}

),

unpacked as (

    select
        event_id,
        payload -> 'properties'                as props,
        payload -> 'geometry' -> 'coordinates' as coords,
        source,
        ingested_at,
        updated_at
    from source

)

select
    event_id,

    -- USGS reports epoch milliseconds; Postgres wants seconds.
    to_timestamp((props ->> 'time')::bigint / 1000.0)    as occurred_at,
    to_timestamp((props ->> 'updated')::bigint / 1000.0) as revised_at,

    nullif(props ->> 'place', '')   as place_raw,
    nullif(props ->> 'magType', '') as magnitude_type,
    nullif(props ->> 'status', '')  as review_status,
    nullif(props ->> 'type', '')    as event_type,
    nullif(props ->> 'url', '')     as usgs_url,

    (props ->> 'mag')::numeric  as magnitude,
    (props ->> 'sig')::integer  as significance,
    (props ->> 'felt')::integer as felt_reports,

    -- GeoJSON orders coordinates [longitude, latitude, depth].
    (coords ->> 0)::numeric as longitude,
    (coords ->> 1)::numeric as latitude,
    (coords ->> 2)::numeric as depth_km,

    (props ->> 'tsunami')::integer = 1 as has_tsunami_flag,

    source,
    ingested_at,
    updated_at

from unpacked
-- A feature with no origin time is unusable downstream and only ever appears
-- when USGS publishes a placeholder for an event still being located.
where props ->> 'time' is not null
