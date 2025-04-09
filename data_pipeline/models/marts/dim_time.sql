with base as (
    select distinct trip_timestamp
    from {{ ref('stg_trips_raw') }}
)

select
    trip_timestamp,
    -- Get date component using macro
    {{ parse_custom_timestamp('trip_timestamp', 'date') }} as date,
    -- Get time component using macro
    {{ parse_custom_timestamp('trip_timestamp', 'time') }} as time,
    -- Get year segment using macro
    {{ parse_custom_timestamp('trip_timestamp', 'year_segment') }} as year_segment
from base