{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
with
    test as (
        select
            command_invocation_id,
            node_id,
            resource_type,
            project,
            resource_name,
            name,
            description,
            depends_on_nodes,
            package_name,
            test_path,
            test_layer,
            tags,
            test_metadata,
            test_package,
            test_name,
            test_model,
            test_column,
            test_column_value,
            test_column_min_value,
            test_column_max_value,
            test_relationship_from_model_condition,
            test_to_model,
            test_relationship_to_field,
            test_relationship_to_model_condition,
            test_column_expression
        from {{ ref('dbt_observability_marts', 'int_test') }}
    ),

    -- one model per test so dim_test stays one row per test; relationship tests
    -- cover two models -- the secondary is kept as test_to_model.
    test_model_map as (
        select
            command_invocation_id as map_invocation_id,
            test_node_id as map_node_id,
            model_key,
            row_number() over (
                partition by command_invocation_id, test_node_id
                order by model_node_id
            ) as model_rank
        from {{ ref('dbt_observability_marts', 'int_test_model') }}
    )

select
    {{ dbt_utils.generate_surrogate_key([
        'command_invocation_id',
        'node_id'
    ]) }} as test_key,
    {{ dbt_utils.generate_surrogate_key([
        'command_invocation_id'
    ]) }} as invocation_key,
    test_model_map.model_key,
    node_id,
    resource_type,
    project,
    resource_name,
    name,
    description,
    depends_on_nodes,
    test_path,
    test_layer,
    tags,
    test_metadata,
    test_package,
    test_name,
    test_model,
    test_column,
    test_column_value,
    test_column_min_value,
    test_column_max_value,
    test_relationship_from_model_condition,
    test_to_model,
    test_relationship_to_field,
    test_relationship_to_model_condition,
    test_column_expression
from test
left outer join test_model_map
    on test.command_invocation_id = test_model_map.map_invocation_id
        and test.node_id = test_model_map.map_node_id
        and test_model_map.model_rank = 1
