{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
select
    command_invocation_id,
    node_id,
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=1) }} as resource_type,
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=2) }} as project,
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=3) }} as resource_name,
    run_started_at,
    name,
    description,
    depends_on_nodes,
    package_name,
    test_path,
    {{ dbt.split_part(string_text='test_path', delimiter_text="'/'", part_number=2) }} as test_layer,
    tags,
    test_metadata,
    test_package,
    test_name,
    test_model,
    column_name as test_column,
    test_combination_of_columns,
    test_column_value,
    {{ dbt.safe_cast('test_column_min_value', dbt.type_int()) }} as test_column_min_value,
    {{ dbt.safe_cast('test_column_max_value', dbt.type_int()) }} as test_column_max_value,
    test_relationship_from_model_condition,
    test_to_model,
    test_relationship_to_field,
    test_relationship_to_model_condition,
    test_column_expression
from {{ ref('dbt_observability_marts', 'stg_test') }}
