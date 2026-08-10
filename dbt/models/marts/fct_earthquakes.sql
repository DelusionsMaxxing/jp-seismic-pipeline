{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['occurred_at']},
            {'columns': ['region_key']},
        ]
    )
}}

-- Event-grain fact table, rebuilt in full on every run. The feed produces a
-- few events a day, so a full rebuild is bounded by the source table rather
-- than by anything this model does: measured against Postgres 16 it takes
-- 0.14s at 322 rows, 0.48s at 100k and 3.2s at 1M. Reaching the ten-minute
-- mark that would justify incrementality needs a couple of hundred million
-- rows — centuries of history at the observed rate.
--
-- Rebuilding also means USGS revisions land without a lookback window: the
-- reviewed solution simply replaces the automatic one on the next run.

with enriched as (

    select * from {{ ref('int_earthquakes_enriched') }}

),

regions as (

    select * from {{ ref('dim_region') }}

)

select
    e.event_id,
    r.region_key,

    e.occurred_at,
    e.occurred_on_jst,
    e.revised_at,

    e.magnitude,
    e.magnitude_type,
    e.magnitude_band,
    e.depth_km,
    e.depth_band,
    e.latitude,
    e.longitude,

    e.significance,
    e.felt_reports,
    e.has_tsunami_flag,
    e.is_potentially_damaging,

    e.place_raw,
    e.event_type,
    e.review_status,
    e.usgs_url,

    e.ingested_at,
    e.updated_at

from enriched e
left join regions r on e.region_name = r.region_name
