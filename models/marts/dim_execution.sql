{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
with
    executions as (
        select
            command_invocation_id,
            node_id,
            resource_type,
            project,
            resource_name,
            status,
            materialization,
            schema_name,
            run_started_at,
            was_full_refresh,
            thread_id,
            compile_started_at,
            query_completed_at,
            rank() over (order by run_started_at desc) as run_rank,
            rank() over (partition by node_id order by run_started_at desc) as node_rank,
            rank() over (partition by node_id, cast(run_started_at as date) order by run_started_at desc) as node_rank_per_day

        from {{ ref('int_execution') }}
    )

select
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as execution_key,
    command_invocation_id,
    run_started_at,
    compile_started_at,
    query_completed_at,
    was_full_refresh,
    thread_id,
    node_id,
    resource_type,
    project,
    resource_name,
    status,
    materialization,
    schema_name,
    run_rank,
    node_rank,
    node_rank_per_day,
    case
        when run_rank = 1 then 'Yes'
        else 'No'
    end as most_recent_run,
    case
        when node_rank = 1 then 'Yes'
        else 'No'
    end as most_recent_node_run,
    case
        when node_rank_per_day = 1 then 'Yes'
        else 'No'
    end as most_recent_node_run_per_day
from executions
