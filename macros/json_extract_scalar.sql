{#-
    Extract a single scalar value from a JSON document at the given path.

    Usage:
        {{ json_extract_scalar('test_metadata', ['kwargs', 'column_name']) }}

    `path_parts` is a list of keys describing the path from the document root.

    Cross-adapter notes:
      - Snowflake stores `test_metadata` as a native VARIANT, so it is accessed
        directly with the `:` path operator (no parse step).
      - Most other adapters store the document as a JSON string and use their
        SQL/JSON path functions; Postgres casts to jsonb first.
      - The returned value is always a string; cast downstream as needed.
-#}

{% macro json_extract_scalar(json_column, path_parts) -%}
    {{ return(adapter.dispatch('json_extract_scalar', 'dbt_observability_marts')(json_column, path_parts)) }}
{%- endmacro %}


{#- Default: SQL/JSON standard JSON_VALUE (SQL Server, Oracle, DuckDB, MySQL, Postgres 17+) -#}
{% macro default__json_extract_scalar(json_column, path_parts) -%}
    {%- set jsonpath = '$.' ~ (path_parts | join('.')) -%}
    json_value({{ json_column }}, '{{ jsonpath }}')
{%- endmacro %}


{% macro snowflake__json_extract_scalar(json_column, path_parts) -%}
    {%- set path = path_parts | join(':') -%}
    {{ json_column }}:{{ path }}::varchar
{%- endmacro %}


{% macro bigquery__json_extract_scalar(json_column, path_parts) -%}
    {%- set jsonpath = '$.' ~ (path_parts | join('.')) -%}
    json_value({{ json_column }}, '{{ jsonpath }}')
{%- endmacro %}


{% macro spark__json_extract_scalar(json_column, path_parts) -%}
    {%- set jsonpath = '$.' ~ (path_parts | join('.')) -%}
    get_json_object({{ json_column }}, '{{ jsonpath }}')
{%- endmacro %}


{% macro databricks__json_extract_scalar(json_column, path_parts) -%}
    {{ return(dbt_observability_marts.spark__json_extract_scalar(json_column, path_parts)) }}
{%- endmacro %}


{% macro postgres__json_extract_scalar(json_column, path_parts) -%}
    {%- set path = path_parts | join(',') -%}
    nullif({{ json_column }}::text, '')::jsonb #>> '{ {{- path -}} }'
{%- endmacro %}


{% macro redshift__json_extract_scalar(json_column, path_parts) -%}
    json_extract_path_text(
        {{ json_column }}
        {%- for part in path_parts %}, '{{ part }}'{% endfor -%}
    )
{%- endmacro %}


{% macro duckdb__json_extract_scalar(json_column, path_parts) -%}
    {%- set jsonpath = '$.' ~ (path_parts | join('.')) -%}
    json_extract_string({{ json_column }}, '{{ jsonpath }}')
{%- endmacro %}
