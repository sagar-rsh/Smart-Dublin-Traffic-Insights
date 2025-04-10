select
    junction_id,
    x_coord,
    y_coord,
    junction_name,
    case
        when x_coord is null or y_coord is null then 'Unknown Location'
        else junction_name || ', ' || cast(x_coord as text) || ', ' || cast(y_coord as text)
    end as full_location
from {{ ref('stg_junctions') }}
