{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
-- Edge list mapping each test to the model(s) it covers, derived from the
-- test's depends_on_nodes. One row per test-covers-model pair (relationship
-- tests produce two rows). Consumed by dim_model.is_tested and dim_test.model_key.
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

    models as (
        select
            command_invocation_id,
            node_id as model_node_id,
            {{ dbt_utils.generate_surrogate_key([
                'command_invocation_id', 'node_id'
            ]) }} as model_key,
            {{ dbt.concat(["'%'", 'lower(node_id)', "'%'"]) }} as node_key
        from {{ ref('dbt_observability_marts', 'int_model') }}
    )

select
    tests.command_invocation_id,
    tests.test_node_id,
    tests.test_key,
    models.model_node_id,
    models.model_key
from tests
inner join models
    on tests.command_invocation_id = models.command_invocation_id
        and tests.depends_on_nodes like models.node_key
