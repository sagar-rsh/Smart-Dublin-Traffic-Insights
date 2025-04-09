select
    route::int,
    link::int,
    direction::int,
    tcs1::int,
    tcs2::int,
    wkt,
    case 
        when wkt is null then 'UNKNOWN'
        else wkt
    end as wkt_clean
from {{ source('public', 'routes') }}
where route is not null and link is not null  -- Ensuring no null routes or links