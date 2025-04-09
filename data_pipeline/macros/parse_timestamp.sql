{% macro parse_custom_timestamp(trip_timestamp_column, component='all') %}
    {% if component == 'date' %}
        case 
            when {{ trip_timestamp_column }} is null or {{ trip_timestamp_column }} = '' then NULL
            when {{ trip_timestamp_column }} ~ '^\d{8}-\d{4}$'
            then to_char(to_timestamp({{ trip_timestamp_column }}, 'YYYYMMDD-HH24MI'), 'YYYY-MM-DD')
            else NULL
        end
    {% elif component == 'time' %}
        case 
            when {{ trip_timestamp_column }} is null or {{ trip_timestamp_column }} = '' then NULL
            when {{ trip_timestamp_column }} ~ '^\d{8}-\d{4}$'
            then to_char(to_timestamp({{ trip_timestamp_column }}, 'YYYYMMDD-HH24MI'), 'HH24:MI:SS')
            else NULL
        end
    {% elif component == 'year_segment' %}
        case 
            when {{ trip_timestamp_column }} ~ '^\d{8}-\d{4}$' and 
                 to_char(to_timestamp({{ trip_timestamp_column }}, 'YYYYMMDD-HH24MI'), 'YYYY') = '2025' 
            then '2025'
            when {{ trip_timestamp_column }} ~ '^\d{8}-\d{4}$' 
            then 'Other Year'
            else NULL
        end
    {% else %}
        (
            select
                case 
                    when {{ trip_timestamp_column }} is null or {{ trip_timestamp_column }} = '' then NULL
                    when {{ trip_timestamp_column }} ~ '^\d{8}-\d{4}$'
                    then to_char(to_timestamp({{ trip_timestamp_column }}, 'YYYYMMDD-HH24MI'), 'YYYY-MM-DD')
                    else NULL
                end as date,
                case 
                    when {{ trip_timestamp_column }} is null or {{ trip_timestamp_column }} = '' then NULL
                    when {{ trip_timestamp_column }} ~ '^\d{8}-\d{4}$'
                    then to_char(to_timestamp({{ trip_timestamp_column }}, 'YYYYMMDD-HH24MI'), 'HH24:MI:SS')
                    else NULL
                end as time,
                case 
                    when {{ trip_timestamp_column }} ~ '^\d{8}-\d{4}$' and 
                         to_char(to_timestamp({{ trip_timestamp_column }}, 'YYYYMMDD-HH24MI'), 'YYYY') = '2025' 
                    then '2025'
                    when {{ trip_timestamp_column }} ~ '^\d{8}-\d{4}$' 
                    then 'Other Year'
                    else NULL
                end as year_segment
        )
    {% endif %}
{% endmacro %}