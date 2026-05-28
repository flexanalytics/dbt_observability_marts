{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
-- Edge list mapping each test to the model OR source it covers, derived from
-- the test's depends_on_nodes. One row per test-covers-node pair (relationship
-- tests produce two rows). model_key is populated for model-targeted tests,
-- source_key for source-targeted tests. Consumed by dim_model.is_tested,
-- dim_source.is_tested, and dim_test.model_key / source_key.
with
    tests as (
        select
            command_invocation_id,
            node_id as test_node_id,
            {{ dbt_utils.generate_surrogate_key([
                'command_invocation_id', 'node_id'
            ]) }} as test_key,
            lower(
                {% if target.type in ('snowflake', 'bigquery') %}
                    array_to_string(depends_on_nodes, '')
                {% else %}
                depends_on_nodes
            {% endif %})
                as depends_on_nodes
        from {{ ref('dbt_observability_marts', 'stg_test') }}
    ),

    nodes as (
        select
            command_invocation_id,
            node_id as tested_node_id,
            'model' as tested_resource_type,
            {{ dbt_utils.generate_surrogate_key([
                'command_invocation_id', 'node_id'
            ]) }} as model_key,
            cast(null as {{ dbt.type_string() }}) as source_key,
            {{ dbt.concat(["'%'", 'lower(node_id)', "'%'"]) }} as node_key
        from {{ ref('dbt_observability_marts', 'int_model') }}
        union all
        select
            command_invocation_id,
            node_id as tested_node_id,
            'source' as tested_resource_type,
            cast(null as {{ dbt.type_string() }}) as model_key,
            {{ dbt_utils.generate_surrogate_key([
                'command_invocation_id', 'node_id'
            ]) }} as source_key,
            {{ dbt.concat(["'%'", 'lower(node_id)', "'%'"]) }} as node_key
        from {{ ref('dbt_observability_marts', 'int_source') }}
    )

select
    tests.command_invocation_id,
    tests.test_node_id,
    tests.test_key,
    nodes.tested_node_id,
    nodes.tested_resource_type,
    nodes.model_key,
    nodes.source_key
from tests
inner join nodes
    on tests.command_invocation_id = nodes.command_invocation_id
        and tests.depends_on_nodes like nodes.node_key
