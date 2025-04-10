{% macro parse_custom_timestamp(trip_timestamp_column, component='all') %}
    {% if component == 'date' %}
        case
            when {{ trip_timestamp_column }} is null or {{ trip_timestamp_column }} = '' then NULL
            when {{ trip_timestamp_column }} ~ '^[0-9]{8}-[0-9]{4}$'
            then to_char(to_timestamp({{ trip_timestamp_column }}, 'YYYYMMDD-HH24MI'), 'YYYY-MM-DD')
            else NULL
        end

    {% elif component == 'time' %}
        case
            when {{ trip_timestamp_column }} is null or {{ trip_timestamp_column }} = '' then NULL
            when {{ trip_timestamp_column }} ~ '^[0-9]{8}-[0-9]{4}$'
            then to_char(to_timestamp({{ trip_timestamp_column }}, 'YYYYMMDD-HH24MI'), 'HH24:MI:SS')
            else NULL
        end

    {% elif component == 'year_segment' %}
        case
            when {{ trip_timestamp_column }} is null or {{ trip_timestamp_column }} = '' then NULL
            when {{ trip_timestamp_column }} ~ '^[0-9]{8}-[0-9]{4}$'
            then to_char(to_timestamp({{ trip_timestamp_column }}, 'YYYYMMDD-HH24MI'), 'YYYY')
            else NULL
        end

    {% else %}
        (
            select
                to_char(to_timestamp({{ trip_timestamp_column }}, 'YYYYMMDD-HH24MI'), 'YYYY-MM-DD') as date,
                to_char(to_timestamp({{ trip_timestamp_column }}, 'YYYYMMDD-HH24MI'), 'HH24:MI:SS') as time,
                to_char(to_timestamp({{ trip_timestamp_column }}, 'YYYYMMDD-HH24MI'), 'YYYY') as year_segment
            where {{ trip_timestamp_column }} is not null
              and {{ trip_timestamp_column }} != ''
              and {{ trip_timestamp_column }} ~ '^[0-9]{8}-[0-9]{4}$'
        )
    {% endif %}
{% endmacro %}
