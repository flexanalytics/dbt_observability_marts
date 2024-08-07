with rowcount as (
    select
        all_executions.command_invocation_id,
        all_executions.node_id,
        all_executions.run_started_at,
        models.total_rowcount,
        lag(models.total_rowcount, 0)
            over (order by all_executions.run_started_at desc)
            as current_rowcount,
        lead(models.total_rowcount, 1)
            over (order by all_executions.run_started_at desc)
            as previous_rowcount
    from edw_observability.all_executions as all_executions
    inner join edw_observability.models as models
        on
            all_executions.command_invocation_id = models.command_invocation_id
            and all_executions.node_id = models.node_id
    where all_executions.status = 'success'
),

rowdiff as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['command_invocation_id', 'node_id']
        ) }}
            as execution_key,
        command_invocation_id,
        node_id,
        run_started_at,
        total_rowcount,
        current_rowcount,
        previous_rowcount,
        current_rowcount - previous_rowcount as rowcount_diff,
        (
            cast(current_rowcount as numeric(19, 6))
            - cast(previous_rowcount as numeric(19, 6))
        )
        / nullif(cast(previous_rowcount as numeric(19, 6)), 0) as rowcount_diff_pct
    from rowcount
)

select *
from rowdiff
where
    abs(rowcount_diff_pct)
    > {{ var('dbt_observability:rowcount_diff_threshold_pct', '.05') }}
