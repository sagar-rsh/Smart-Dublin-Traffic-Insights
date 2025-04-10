with base as (
    select distinct trip_timestamp
    from {{ ref('stg_trips_raw') }}
)

select
    trip_timestamp,
    {{ parse_custom_timestamp('trip_timestamp', 'date') }} as date,
    {{ parse_custom_timestamp('trip_timestamp', 'time') }} as time,
    {{ parse_custom_timestamp('trip_timestamp', 'year_segment') }} as year_segment
from base