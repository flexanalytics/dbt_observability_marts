{#
    Return the lowercased column names of `relation`, or [] when it cannot be
    inspected (parse phase, or the relation does not exist yet -- e.g. building a
    model in isolation before its upstream). Cross-adapter: relies only on
    load_relation + adapter.get_columns_in_relation.
#}
{% macro relation_columns(relation) -%}
    {%- set columns = [] -%}
    {%- if execute -%}
        {%- set loaded = load_relation(relation) -%}
        {%- if loaded is not none -%}
            {%- set columns = adapter.get_columns_in_relation(loaded)
                | map(attribute='name') | map('lower') | list -%}
        {%- endif -%}
    {%- endif -%}
    {{ return(columns) }}
{%- endmacro %}

{#
    Render a column reference that tolerates the column being absent from `relation`.

    Returns the bare column when present, otherwise
    `cast(null as <data_type>) as <column_name>`. This lets a staging model stay
    compatible with upstream observability tables written before a column was added
    -- when the column is missing we emit null.

    Pass `available_columns` (from relation_columns) to avoid re-inspecting the
    relation once per column; when omitted it is looked up per call.

    Usage:
        {% set cols = relation_columns(my_relation) %}
        {{ optional_column(my_relation, 'column_name', dbt.type_string(), cols) }}
#}
{% macro optional_column(relation, column_name, data_type=none, available_columns=none) -%}
    {%- set data_type = data_type if data_type is not none else dbt.type_string() -%}
    {%- set available_columns = available_columns if available_columns is not none
        else relation_columns(relation) -%}
    {%- if (column_name | lower) in available_columns -%}
        {{ column_name }}
    {%- else -%}
        cast(null as {{ data_type }}) as {{ column_name }}
    {%- endif -%}
{%- endmacro %}
