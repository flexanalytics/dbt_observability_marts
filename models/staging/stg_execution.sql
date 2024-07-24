{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
{% set ref_union = (var('dbt_observability:objects', none) is not none) %}
select
    command_invocation_id,
    node_id,
    run_started_at,
    was_full_refresh,
    thread_id,
    status,
    compile_started_at,
    query_completed_at,
    total_node_runtime,
    materialization,
    schema_name,
    name,
    resource_type
{% if ref_union %}
from {{ ref('dbt_observability_marts', 'all_executions') }}
{% else %}
from {{ ref('dbt_observability', 'all_executions') }}
{% endif %}

