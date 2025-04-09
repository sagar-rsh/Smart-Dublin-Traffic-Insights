with trip_data as (
    select
        t.trip_timestamp,
        t.route,
        t.link,
        t.direction,
        t.stt,
        t.acc_stt,
        t.tcs1,
        t.tcs2,
        r.wkt_clean,
        j1.junction_name as junction_start,
        j2.junction_name as junction_end,
        case 
            when t.stt > 300 then 'Long Trip'
            when t.stt <= 300 then 'Short Trip'
            else 'Unknown'
        end as trip_type
    from {{ ref('stg_trips_raw') }} t
    left join {{ ref('dim_routes') }} r
        on t.route = r.route and t.link = r.link and t.direction = r.direction
    left join {{ ref('dim_junctions') }} j1
        on t.tcs1 = j1.junction_id
    left join {{ ref('dim_junctions') }} j2
        on t.tcs2 = j2.junction_id
    where t.stt is not null or t.stt is not null
)

-- Aggregation layer for daily statistics
select
    dt.date,
    dt.time,
    td.route,
    count(*) as trip_count,
    avg(td.stt) as avg_travel_time,
    max(td.stt) as max_travel_time,
    min(td.stt) as min_travel_time,
    sum(td.acc_stt) as total_accumulated_stt,
    td.trip_type
from trip_data td
left join {{ ref('dim_time') }} dt
    -- Extract the date and time components from both trip_timestamp and dim_time
    on {{ parse_custom_timestamp('td.trip_timestamp', 'date') }} = dt.date
    and {{ parse_custom_timestamp('td.trip_timestamp', 'time') }} = dt.time
group by dt.date, dt.time, td.route, td.trip_type
