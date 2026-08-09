-- Derives the classification columns that both the fact table and the daily
-- aggregate need, so the banding rules exist in exactly one place.

with quakes as (

    select * from {{ ref('stg_earthquakes') }}

)

select
    *,

    -- USGS formats place as "<distance> <bearing> of <locality>, <region>".
    -- Everything after the final comma is the region; events far offshore
    -- have no comma at all and fall back to the whole string.
    coalesce(
        nullif(trim(regexp_replace(place_raw, '^.*,\s*', '')), ''),
        nullif(trim(place_raw), '')
    ) as region_name,

    -- Wadati-Benioff depth classes, the convention used in seismology for
    -- separating crustal events from those inside the subducting slab.
    case
        when depth_km is null  then 'unknown'
        when depth_km < 70     then 'shallow'
        when depth_km < 300    then 'intermediate'
        else 'deep'
    end as depth_band,

    case
        when magnitude is null then 'unknown'
        when magnitude < 4.0   then 'minor'
        when magnitude < 5.0   then 'light'
        when magnitude < 6.0   then 'moderate'
        when magnitude < 7.0   then 'strong'
        when magnitude < 8.0   then 'major'
        else 'great'
    end as magnitude_band,

    -- Shallow events above M5.5 are the ones that actually shake structures;
    -- a deep M6 off the Kuril trench is usually barely felt on land.
    (
        magnitude >= 5.5
        and depth_km is not null
        and depth_km < 70
    ) as is_potentially_damaging,

    (occurred_at at time zone 'Asia/Tokyo')::date as occurred_on_jst

from quakes
