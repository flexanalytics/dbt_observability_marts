{#
    Render a column reference that tolerates the column being absent from `relation`.

    Returns the bare column when `relation` has it, otherwise
    `cast(null as <data_type>) as <column_name>`. This lets a staging model stay
    compatible with upstream observability tables written before a column was added
    -- when the column is missing we emit null and downstream models fall back
    (e.g. int_test parsing test_metadata.kwargs).

    Relies only on cross-adapter dbt builtins (load_relation,
    adapter.get_columns_in_relation, dbt.type_string), so no dispatch is needed.
    The load_relation guard also keeps it safe when `relation` does not yet exist
    (e.g. building this model in isolation before its upstream).

    Usage:
        {{ optional_column(ref('dbt_observability', 'tests'), 'column_name') }}
        {{ optional_column(my_relation, 'some_int_col', dbt.type_int()) }}
#}
{% macro optional_column(relation, column_name, data_type=none) -%}
    {%- set data_type = data_type if data_type is not none else dbt.type_string() -%}
    {%- set has_column = false -%}
    {%- if execute -%}
        {%- set loaded = load_relation(relation) -%}
        {%- if loaded is not none -%}
            {%- set existing = adapter.get_columns_in_relation(loaded)
                | map(attribute='name') | map('lower') | list -%}
            {%- set has_column = (column_name | lower) in existing -%}
        {%- endif -%}
    {%- endif -%}
    {%- if has_column -%}
        {{ column_name }}
    {%- else -%}
        cast(null as {{ data_type }}) as {{ column_name }}
    {%- endif -%}
{%- endmacro %}
