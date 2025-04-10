with trips_raw as (
    select
        "timestamp" as trip_timestamp,
        route::int,
        link::int,
        direction::int,
        stt::float,
        acc_stt::float,
        tcs1::int,
        tcs2::int
    from {{ source('public', 'trips_raw') }}
    where "timestamp" is not null  -- Filtering out rows with no timestamp
)

-- Normalize travel times (eg: remove outliers or cap extreme values)
select
    trip_timestamp::varchar as trip_timestamp,
    route,
    link,
    direction,
    case 
        when stt < 0 or stt > 600 then null  -- Filtering unreasonable travel times
        else stt
    end as stt,
    case
        when acc_stt < 0 then null  -- Accumulated STT should never be negative
        else acc_stt
    end as acc_stt,
    tcs1,
    tcs2
from trips_raw
