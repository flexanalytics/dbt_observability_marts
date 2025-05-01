{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
with

    executions as (
        select
            command_invocation_id,
            run_started_at,
            node_id,
            resource_type,
            project,
            resource_name,
            min(run_started_at) over () as project_first_run_started_at,
            max(run_started_at)
                over ()
                as project_most_recent_run_started_at,
            min(run_started_at)
                over (partition by node_id)
                as node_first_run_started_at,
            max(run_started_at)
                over (partition by node_id)
                as node_most_recent_run_started_at,
            lag(run_started_at)
                over (
                    partition by node_id
                    order by run_started_at
                )
                as previous_run_started_at,
            lead(run_started_at)
                over (
                    partition by node_id
                    order by run_started_at
                )
                as next_run_started_at
        from {{ ref('dbt_observability_marts', 'int_execution') }}
    ),

    _raw_columns as (
        select distinct
            cols.node_id,
            cols.command_invocation_id,
            cols.column_name,
            cols.data_type as raw_data_type,
            excs.resource_type,
            excs.project,
            excs.resource_name,
            excs.run_started_at,
            min(excs.run_started_at)
                over (partition by cols.node_id)
                as node_first_run_started_at,
            max(excs.run_started_at)
                over (partition by cols.node_id)
                as node_most_recent_run_started_at,
            lag(excs.run_started_at)
                over (
                    partition by cols.node_id, cols.column_name
                    order by excs.run_started_at
                )
                as column_previous_run_started_at,
            lead(excs.run_started_at)
                over (
                    partition by cols.node_id, cols.column_name
                    order by excs.run_started_at
                )
                as column_next_run_started_at,
            lag(cols.data_type)
                over (
                    partition by cols.node_id, cols.column_name
                    order by excs.run_started_at
                )
                as raw_pre_data_type
        from {{ ref('dbt_observability_marts', 'stg_column') }} as cols
        inner join executions
            as excs on cols.command_invocation_id = excs.command_invocation_id
            and cols.node_id = excs.node_id
    ),

    columns as (
        select
            node_id,
            command_invocation_id,
            column_name,
            {{ convert_to_generic_type('raw_data_type') }} as generic_data_type,
            {{ convert_to_generic_type('raw_pre_data_type') }}
                as generic_pre_data_type,
            raw_data_type as precise_data_type,
            raw_pre_data_type as precise_pre_data_type,
            resource_type,
            project,
            resource_name,
            run_started_at,
            node_first_run_started_at,
            node_most_recent_run_started_at,
            column_previous_run_started_at,
            column_next_run_started_at
        from _raw_columns
    ),

    -- models, seeds, snapshots, tests, and sources
    added as (
        select
            node_id,
            command_invocation_id,
            resource_type,
            project,
            resource_name,
            run_started_at,
            '' as column_name,
            '' as generic_data_type,
            '' as generic_pre_data_type,
            '' as precise_data_type,
            '' as precise_pre_data_type,
            {{ dbt.concat(['resource_type', "'_added'"]) }} as change_type,
            {{ dbt.concat([
                "upper(substring(resource_type, 1, 1))",
                "lower(substring(resource_type, 2, " ~ dbt.length('resource_type') ~ "))",
                "' Added'"
            ]) }} as change_type_desc,
            run_started_at as detected_at
        from
            executions
        where
            previous_run_started_at is null
            and run_started_at > project_first_run_started_at
    ),

    first_observed as (
        select
            node_id,
            command_invocation_id,
            resource_type,
            project,
            resource_name,
            run_started_at,
            '' as column_name,
            '' as generic_data_type,
            '' as generic_pre_data_type,
            '' as precise_data_type,
            '' as precise_pre_data_type,
            {{ dbt.concat(['resource_type', "'_first_observed'"]) }} as change_type,
            {{ dbt.concat([
                "upper(substring(resource_type, 1, 1))",
                "lower(substring(resource_type, 2, " ~ dbt.length('resource_type') ~ "))",
                "' First Observed'"
            ]) }} as change_type_desc,
            run_started_at as detected_at
        from
            executions
        where
            previous_run_started_at is null
            and run_started_at = project_first_run_started_at
    ),


    removed as (
        select
            node_id,
            command_invocation_id,
            resource_type,
            project,
            resource_name,
            run_started_at,
            '' as column_name,
            '' as generic_data_type,
            '' as generic_pre_data_type,
            '' as precise_data_type,
            '' as precise_pre_data_type,
            {{ dbt.concat(['resource_type', "'_removed'"]) }} as change_type,
            {{ dbt.concat([
                "upper(substring(resource_type, 1, 1))",
                "lower(substring(resource_type, 2, " ~ dbt.length('resource_type') ~ "))",
                "' Removed'"
            ]) }} as change_type_desc,
            run_started_at as detected_at
        from
            executions
        where
            next_run_started_at is null
            and run_started_at < node_most_recent_run_started_at
            and run_started_at < project_most_recent_run_started_at
    ),

    -- columns
    columns_added as (
        select
            node_id,
            command_invocation_id,
            resource_type,
            project,
            resource_name,
            'column_added' as change_type,
            'Column Added' as change_type_desc,
            run_started_at,
            column_name,
            generic_data_type,
            generic_pre_data_type,
            precise_data_type,
            precise_pre_data_type,
            run_started_at as detected_at
        from columns
        where
            column_previous_run_started_at is null
            and run_started_at > node_first_run_started_at
    ),

    columns_first_observed as (
        select
            node_id,
            command_invocation_id,
            resource_type,
            project,
            resource_name,
            'column_first_observed' as change_type,
            'Column First Observed' as change_type_desc,
            run_started_at,
            column_name,
            generic_data_type,
            generic_pre_data_type,
            precise_data_type,
            precise_pre_data_type,
            run_started_at as detected_at
        from columns
        where
            column_previous_run_started_at is null
            and run_started_at = node_first_run_started_at
    ),


    columns_removed as (
        select
            node_id,
            command_invocation_id,
            resource_type,
            project,
            resource_name,
            'column_removed' as change_type,
            'Column Removed' as change_type_desc,
            run_started_at,
            column_name,
            generic_data_type,
            generic_pre_data_type,
            precise_data_type,
            precise_pre_data_type,
            run_started_at as detected_at
        from columns
        where
            column_next_run_started_at is null
            and run_started_at < node_most_recent_run_started_at
    ),

    -- type changes
    type_changes as (
        select
            node_id,
            command_invocation_id,
            'column' as resource_type,
            project,
            resource_name,
            'type_changed' as change_type,
            'Type Changed' as change_type_desc,
            run_started_at,
            column_name,
            generic_data_type,
            generic_pre_data_type,
            precise_data_type,
            precise_pre_data_type,
            run_started_at as detected_at
        from columns
        where generic_data_type != generic_pre_data_type
        union all
        select
            node_id,
            command_invocation_id,
            'column' as resource_type,
            project,
            resource_name,
            'precision_changed' as change_type,
            'Precision Changed' as change_type_desc,
            run_started_at,
            column_name,
            generic_data_type,
            generic_pre_data_type,
            precise_data_type,
            precise_pre_data_type,
            run_started_at as detected_at
        from columns
        where precise_data_type != precise_pre_data_type
    ),

    all_changes as (
        select
            node_id,
            command_invocation_id,
            resource_type,
            project,
            resource_name,
            change_type,
            change_type_desc,
            column_name,
            generic_data_type,
            generic_pre_data_type,
            precise_data_type,
            precise_pre_data_type,
            detected_at
        from added
        union all
        select
            node_id,
            command_invocation_id,
            resource_type,
            project,
            resource_name,
            change_type,
            change_type_desc,
            column_name,
            generic_data_type,
            generic_pre_data_type,
            precise_data_type,
            precise_pre_data_type,
            detected_at
        from first_observed
        union all
        select
            node_id,
            command_invocation_id,
            resource_type,
            project,
            resource_name,
            change_type,
            change_type_desc,
            column_name,
            generic_data_type,
            generic_pre_data_type,
            precise_data_type,
            precise_pre_data_type,
            detected_at
        from removed
        union all
        select
            node_id,
            command_invocation_id,
            resource_type,
            project,
            resource_name,
            change_type,
            change_type_desc,
            column_name,
            generic_data_type,
            generic_pre_data_type,
            precise_data_type,
            precise_pre_data_type,
            detected_at
        from columns_added
        union all
        select
            node_id,
            command_invocation_id,
            resource_type,
            project,
            resource_name,
            change_type,
            change_type_desc,
            column_name,
            generic_data_type,
            generic_pre_data_type,
            precise_data_type,
            precise_pre_data_type,
            detected_at
        from columns_first_observed
        union all
        select
            node_id,
            command_invocation_id,
            resource_type,
            project,
            resource_name,
            change_type,
            change_type_desc,
            column_name,
            generic_data_type,
            generic_pre_data_type,
            precise_data_type,
            precise_pre_data_type,
            detected_at
        from columns_removed
        union all
        select
            node_id,
            command_invocation_id,
            resource_type,
            project,
            resource_name,
            change_type,
            change_type_desc,
            column_name,
            generic_data_type,
            generic_pre_data_type,
            precise_data_type,
            precise_pre_data_type,
            detected_at
        from type_changes
    )

select distinct
    {{ dbt_utils.generate_surrogate_key(["command_invocation_id", "node_id"]) }}
        as execution_key,
    node_id,
    command_invocation_id,
    resource_type,
    project,
    resource_name,
    change_type,
    change_type_desc,
    column_name,
    generic_data_type,
    generic_pre_data_type,
    precise_data_type,
    precise_pre_data_type,
    detected_at
from all_changes
