{{ config(materialized='table') }}

-- Daily rollup on Japan Standard Time, which is the calendar anyone reading
-- a report about Japan expects. Rebuilt in full: the table is one row per
-- day per region and stays small enough that incrementality would only add
-- complexity.

with events as (

    select * from {{ ref('fct_earthquakes') }}

),

daily as (

    select
        occurred_on_jst as activity_date,
        region_key,

        count(*)                                                as event_count,
        count(*) filter (where is_potentially_damaging)         as damaging_event_count,
        count(*) filter (where has_tsunami_flag)                as tsunami_flagged_count,

        max(magnitude) as max_magnitude,
        avg(magnitude) as avg_magnitude,

        -- percentile_cont has no numeric overload in Postgres: it takes and
        -- returns double precision, which round(x, 2) does not accept. The
        -- parentheses matter — without them the cast binds to the ORDER BY
        -- expression instead of to the aggregate's result.
        (percentile_cont(0.5) within group (order by magnitude))::numeric
            as median_magnitude,

        min(depth_km)                                           as min_depth_km,
        avg(depth_km)                                           as avg_depth_km,

        sum(significance)                                       as total_significance

    from events
    group by 1, 2

)

select
    d.activity_date,
    d.region_key,
    r.region_name,
    r.is_onshore_japan,

    d.event_count,
    d.damaging_event_count,
    d.tsunami_flagged_count,

    round(d.max_magnitude, 2)    as max_magnitude,
    round(d.avg_magnitude, 2)    as avg_magnitude,
    round(d.median_magnitude, 2) as median_magnitude,

    round(d.min_depth_km, 1)     as min_depth_km,
    round(d.avg_depth_km, 1)     as avg_depth_km,

    d.total_significance

from daily d
left join {{ ref('dim_region') }} r on d.region_key = r.region_key
