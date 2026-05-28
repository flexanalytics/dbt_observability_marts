{#-
    Extract a JSON array from a document at the given path, returned as its raw
    JSON text (e.g. '["gi03", "sb00"]'). Companion to json_extract_scalar --
    scalar extractors (json_value, Snowflake `::varchar`) return null for array
    nodes, so arrays need the "query"/array variants below.

    Usage:
        {{ json_extract_array('test_metadata', ['kwargs', 'combination_of_columns']) }}
-#}

{% macro json_extract_array(json_column, path_parts) -%}
    {{ return(adapter.dispatch('json_extract_array', 'dbt_observability_marts')(json_column, path_parts)) }}
{%- endmacro %}


{#- Default: SQL/JSON standard JSON_QUERY returns the array fragment (SQL Server, Oracle, …) -#}
{% macro default__json_extract_array(json_column, path_parts) -%}
    {%- set jsonpath = '$.' ~ (path_parts | join('.')) -%}
    json_query({{ json_column }}, '{{ jsonpath }}')
{%- endmacro %}


{% macro snowflake__json_extract_array(json_column, path_parts) -%}
    {%- set path = path_parts | join(':') -%}
    to_json({{ json_column }}:{{ path }})
{%- endmacro %}


{% macro bigquery__json_extract_array(json_column, path_parts) -%}
    {%- set jsonpath = '$.' ~ (path_parts | join('.')) -%}
    json_query({{ json_column }}, '{{ jsonpath }}')
{%- endmacro %}


{% macro spark__json_extract_array(json_column, path_parts) -%}
    {%- set jsonpath = '$.' ~ (path_parts | join('.')) -%}
    get_json_object({{ json_column }}, '{{ jsonpath }}')
{%- endmacro %}


{% macro databricks__json_extract_array(json_column, path_parts) -%}
    {{ return(dbt_observability_marts.spark__json_extract_array(json_column, path_parts)) }}
{%- endmacro %}


{% macro postgres__json_extract_array(json_column, path_parts) -%}
    {%- set path = path_parts | join(',') -%}
    nullif({{ json_column }}::text, '')::jsonb #>> '{ {{- path -}} }'
{%- endmacro %}


{% macro redshift__json_extract_array(json_column, path_parts) -%}
    json_extract_path_text(
        {{ json_column }}
        {%- for part in path_parts %}, '{{ part }}'{% endfor -%}
    )
{%- endmacro %}


{% macro duckdb__json_extract_array(json_column, path_parts) -%}
    {%- set jsonpath = '$.' ~ (path_parts | join('.')) -%}
    cast(json_extract({{ json_column }}, '{{ jsonpath }}') as varchar)
{%- endmacro %}
