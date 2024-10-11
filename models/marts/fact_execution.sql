{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
with
    executions as (
        select
            e.command_invocation_id,
            e.node_id,
            e.resource_type,
            e.project,
            e.resource_name,
            e.run_started_at,
            cast(
                {{
                    dbt.date_trunc(
                        'day', 'e.run_started_at'
                        )
                    }} as date
            ) as run_start_day,
            e.was_full_refresh,
            e.thread_id,
            e.status,
            e.compile_started_at,
            e.query_completed_at,
            e.total_node_runtime,
            e.materialization,
            e.schema_name,
            case
                when e.resource_type = 'model' then coalesce(cast(m.total_rowcount as {{ api.Column.translate_type('bigint') }}), 0)
                when e.resource_type = 'source' then coalesce(cast(s.total_rowcount as {{ api.Column.translate_type('bigint') }}), 0)
                else 0
            end
            as total_rowcount
        from
            {{ ref('dbt_observability_marts', 'int_execution') }} e
        left outer join {{ ref('dbt_observability_marts', 'stg_model') }} m on
            e.command_invocation_id = m.command_invocation_id
            and e.node_id = m.node_id
        left outer join {{ ref('dbt_observability_marts', 'stg_source') }} s on
            e.command_invocation_id = s.command_invocation_id
            and e.node_id = s.node_id
    )
select
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as execution_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id']) }} as invocation_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as model_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as column_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as test_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as seed_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as source_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as snapshot_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as metric_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as exposure_key,
    {{ dbt_utils.generate_surrogate_key(['node_id']) }} as node_key,
    {{ dbt_utils.generate_surrogate_key(['run_start_day']) }} as date_key,
    run_started_at,
    was_full_refresh,
    thread_id,
    compile_started_at,
    query_completed_at,
    total_node_runtime,
    lag(total_node_runtime) over (partition by node_id order by run_started_at) as previous_runtime,
    avg(total_node_runtime) over (partition by node_id) as average_runtime,
    total_rowcount,
    lag(total_rowcount) over (partition by node_id order by run_started_at) as previous_rowcount,
    avg(total_rowcount) over (partition by node_id) as average_rowcount
from executions
