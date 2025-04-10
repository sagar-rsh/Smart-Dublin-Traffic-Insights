{% macro show_parsed_timestamp() %}
  {% set test_value = "20151119-0608" %}
  
  {% set date_result = parse_custom_timestamp(test_value, "date") %}
  {% set time_result = parse_custom_timestamp(test_value, "time") %}
  {% set year_result = parse_custom_timestamp(test_value, "year_segment") %}
  
  {{ log("=== TEST RESULTS ===", info=true) }}
  {{ log("Input: " ~ test_value, info=true) }}
  {{ log("Date: " ~ date_result, info=true) }}
  {{ log("Time: " ~ time_result, info=true) }}
  {{ log("Year: " ~ year_result, info=true) }}
{% endmacro %}