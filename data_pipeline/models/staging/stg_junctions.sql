select
    SiteID as junction_id,
    X as x_coord,
    Y as y_coord,
    Location as junction_name
from {{ ref('junctions') }}
where SiteID is not null  -- Filtering out junctions with missing SiteID