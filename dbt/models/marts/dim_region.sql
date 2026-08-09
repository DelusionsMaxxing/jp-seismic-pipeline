{{ config(materialized='table') }}

-- Conformed region dimension, derived from the place strings USGS publishes.
-- There is no upstream region reference to join to, so the grain is defined
-- by the distinct labels actually observed in the data.

with regions as (

    select distinct region_name
    from {{ ref('int_earthquakes_enriched') }}
    where region_name is not null

)

select
    {{ dbt_utils.generate_surrogate_key(['region_name']) }} as region_key,
    region_name,

    -- Offshore events are labelled by the sea or trench they occurred in
    -- rather than by a prefecture, and are worth separating in reporting.
    (
        region_name ilike '%japan%'
        and region_name not ilike '%sea of japan%'
    ) as is_onshore_japan,

    region_name ilike any (array[
        '%sea%', '%trench%', '%ridge%', '%rise%', '%ocean%'
    ]) as is_offshore

from regions
