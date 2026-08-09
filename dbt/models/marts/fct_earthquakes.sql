{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns',
        indexes=[
            {'columns': ['occurred_at']},
            {'columns': ['region_key']},
        ]
    )
}}

-- Event-grain fact table. Incremental rather than full-refresh because the
-- history only grows, but the lookback below is deliberately generous:
-- USGS replaces automatic solutions with reviewed ones for days afterwards,
-- and a plain "only new rows" filter would freeze the first, worse estimate.

with enriched as (

    select * from {{ ref('int_earthquakes_enriched') }}

    {% if is_incremental() %}
    where updated_at >= (
        select coalesce(max(updated_at), '1900-01-01'::timestamptz) - interval '7 days'
        from {{ this }}
    )
    {% endif %}

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
