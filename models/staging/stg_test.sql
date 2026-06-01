{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
{% set ref_union = (var('dbt_observability:objects', none) is not none) %}
{% if ref_union %}
    {% set source_relation = ref('dbt_observability_marts', 'tests') %}
{% else %}
    {% set source_relation = ref('dbt_observability', 'tests') %}
{% endif %}
{#
    column_name, attached_node and the parsed test_* columns were added to upstream
    dbt_observability later than the other columns; tolerate their absence in older
    observability data (relation_columns probed once, optional_column emits null when
    a column is missing).
#}
{% set source_columns = relation_columns(source_relation) %}
{% set optional_test_columns = [
    'column_name',
    'attached_node',
    'test_package',
    'test_name',
    'test_model',
    'test_combination_of_columns',
    'test_column_value',
    'test_column_min_value',
    'test_column_max_value',
    'test_relationship_from_model_condition',
    'test_to_model',
    'test_relationship_to_field',
    'test_relationship_to_model_condition',
    'test_column_expression'
] %}
select
    command_invocation_id,
    node_id,
    run_started_at,
    name,
    description,
    depends_on_nodes,
    package_name,
    test_path,
    tags,
    test_metadata
    {%- for column in optional_test_columns %},
    {{ optional_column(source_relation, column, dbt.type_string(), source_columns) }}
    {%- endfor %}
from {{ source_relation }}
