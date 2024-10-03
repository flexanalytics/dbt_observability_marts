{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
with
    invocation as (
        select
            command_invocation_id,
            dbt_version,
            project_name,
            dbt_command,
            full_refresh_flag,
            target_type,
            target_profile_name,
            target_name,
            target_schema,
            target_threads,
            dbt_cloud_project_id,
            dbt_cloud_job_id,
            dbt_cloud_run_id,
            dbt_cloud_run_reason_category,
            dbt_cloud_run_reason,
            env_vars,
            dbt_vars,
            run_started_at,
            rank() over (order by run_started_at desc) as invocation_rank,
            rank() over (
                partition by cast(run_started_at as date)
                order by run_started_at desc
                ) as invocation_rank_per_day
        from {{ ref('stg_invocation') }}
    )

select
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id']) }} as invocation_key,
    command_invocation_id,
    run_started_at,
    invocation_rank,
    case
        when invocation_rank = 1 then 'Yes'
        else 'No'
    end as most_recent_run,
    invocation_rank_per_day,
    case
        when invocation_rank_per_day = 1 then 'Yes'
        else 'No'
    end as most_recent_run_per_day,
    dbt_version,
    project_name,
    dbt_command,
    full_refresh_flag,
    target_type,
    target_profile_name,
    target_name,
    target_schema,
    target_threads,
    dbt_cloud_project_id,
    dbt_cloud_job_id,
    dbt_cloud_run_id,
    dbt_cloud_run_reason_category,
    dbt_cloud_run_reason,
    env_vars,
    dbt_vars
from invocation
