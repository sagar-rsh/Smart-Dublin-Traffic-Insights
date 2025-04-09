select distinct
    route,
    link,
    direction,
    tcs1,
    tcs2,
    wkt_clean,
    case
        when direction = 1 then 'North'
        when direction = 2 then 'South'
        when direction = 3 then 'East'
        when direction = 4 then 'West'
        else 'Unknown'
    end as direction_name
from {{ ref('stg_routes') }}
