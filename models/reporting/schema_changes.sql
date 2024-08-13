{{
    config(
        enabled=var('dbt_observability:marts_enabled', true),
        materialized='incremental'
    )
}}
with

    _executions as (
        select
            command_invocation_id,
            run_started_at,
            node_id,
            name,
            lag(run_started_at) over (partition by node_id order by run_started_at) as previous_run_started_at,
            lead(run_started_at) over (partition by node_id order by run_started_at) as next_run_started_at
        from {{ ref('stg_execution') }}
        {% if is_incremental() %}

        where run_started_at >= (select coalesce(max(detected_at),'1900-01-01') from {{ this }} )

        {% endif %}

    ),

    cur as (
        select distinct
            cols.node_id,
            cols.command_invocation_id,
            cols.column_name,
            cols.data_type,
            excs.run_started_at,
            excs.previous_run_started_at,
            excs.next_run_started_at
        from {{ ref('stg_column') }} as cols
        inner join _executions as excs on cols.command_invocation_id = excs.command_invocation_id
        and cols.node_id = excs.node_id
    ),

    pre as (
        select distinct
            cols.node_id,
            cols.command_invocation_id,
            cols.column_name,
            cols.data_type,
            excs.run_started_at,
            excs.previous_run_started_at,
            excs.next_run_started_at
        from {{ ref('stg_column') }} as cols
        inner join _executions as excs on cols.command_invocation_id = excs.command_invocation_id
        and cols.node_id = excs.node_id
    ),

    cur_models as (
        select distinct
            mdls.node_id,
            mdls.command_invocation_id,
            mdls.run_started_at,
            mdls.name,
            excs.previous_run_started_at,
            excs.next_run_started_at
        from {{ ref('stg_model') }} as mdls
        inner join _executions as excs on mdls.command_invocation_id = excs.command_invocation_id
        and mdls.node_id = excs.node_id
    ),

    pre_models as (
        select distinct
            mdls.node_id,
            mdls.command_invocation_id,
            mdls.run_started_at,
            mdls.name,
            excs.previous_run_started_at,
            excs.next_run_started_at
        from {{ ref('stg_model') }} as mdls
        inner join _executions as excs on mdls.command_invocation_id = excs.command_invocation_id
        and mdls.node_id = excs.node_id
    ),

    cur_seeds as (
        select distinct
            seeds.node_id,
            seeds.command_invocation_id,
            seeds.run_started_at,
            seeds.name,
            excs.previous_run_started_at,
            excs.next_run_started_at
        from {{ ref('stg_seed') }} as seeds
        inner join _executions as excs on seeds.command_invocation_id = excs.command_invocation_id
        and seeds.node_id = excs.node_id
    ),

    pre_seeds as (
        select distinct
            seeds.node_id,
            seeds.command_invocation_id,
            seeds.run_started_at,
            seeds.name,
            excs.previous_run_started_at,
            excs.next_run_started_at
        from {{ ref('stg_seed') }} as seeds
        inner join _executions as excs on seeds.command_invocation_id = excs.command_invocation_id
        and seeds.node_id = excs.node_id
    ),

    cur_sources as (
        select distinct
            sources.node_id,
            sources.command_invocation_id,
            sources.name,
            sources.run_started_at,
            excs.previous_run_started_at,
            excs.next_run_started_at
        from {{ ref('stg_source') }} as sources
        inner join _executions as excs on sources.command_invocation_id = excs.command_invocation_id
        and sources.node_id = excs.node_id
    ),

    pre_sources as (
        select distinct
            sources.node_id,
            sources.command_invocation_id,
            sources.name,
            sources.run_started_at,
            excs.previous_run_started_at,
            excs.next_run_started_at
        from {{ ref('stg_source') }} as sources
        inner join _executions as excs on sources.command_invocation_id = excs.command_invocation_id
        and sources.node_id = excs.node_id
    ),

    cur_snapshots as (
        select distinct
            snaps.node_id,
            snaps.command_invocation_id,
            snaps.name,
            snaps.run_started_at,
            excs.previous_run_started_at,
            excs.next_run_started_at
        from {{ ref('stg_snapshot') }} as snaps
        inner join _executions as excs on snaps.command_invocation_id = excs.command_invocation_id
        and snaps.node_id = excs.node_id
    ),

    pre_snapshots as (
        select distinct
            snaps.node_id,
            snaps.command_invocation_id,
            snaps.name,
            snaps.run_started_at,
            excs.previous_run_started_at,
            excs.next_run_started_at
        from {{ ref('stg_snapshot') }} as snaps
        inner join _executions as excs on snaps.command_invocation_id = excs.command_invocation_id
        and snaps.node_id = excs.node_id
    ),


    type_changes as (
        select
            cur.node_id,
            cur.command_invocation_id,
            'type_changed' as change,
            cur.column_name,
            cur.data_type,
            pre.data_type as pre_data_type,
            pre.next_run_started_at as detected_at
        from pre inner join cur
            on (lower(cur.node_id) = lower(pre.node_id) and lower(cur.column_name) = lower(pre.column_name))
                and pre.run_started_at = cur.previous_run_started_at
        where pre.data_type is not null and lower(cur.data_type) != lower(pre.data_type)
    ),

    columns_added as (
        select
            cur.node_id,
            cur.command_invocation_id,
            'column_added' as change,
            cur.column_name,
            cur.data_type,
            null as pre_data_type,
            cur.run_started_at as detected_at
        from cur
        left outer join
            pre
            on cur.node_id = pre.node_id
                and cur.column_name = pre.column_name
                and cur.previous_run_started_at = pre.run_started_at
        where pre.column_name is null and cur.previous_run_started_at is not null
    ),

    columns_removed as (
        select
            pre.node_id,
            pre.command_invocation_id,
            'column_removed' as change,
            pre.column_name,
            null as data_type,
            pre.data_type as pre_data_type,
            pre.next_run_started_at as detected_at
        from pre
        left outer join
            cur
            on pre.node_id = cur.node_id
                and pre.column_name = cur.column_name
                and pre.next_run_started_at = cur.run_started_at
        where cur.column_name is null
        and pre.next_run_started_at is not null
    ),

    models_added as (
        select
            cur.node_id,
            cur.command_invocation_id,
            'model_added' as change,
            null as column_name,
            null as data_type,
            null as pre_data_type,
            cur.run_started_at as detected_at
        from cur_models cur
        left outer join
            pre_models pre
            on cur.node_id = pre.node_id
                and cur.previous_run_started_at = pre.run_started_at
        where pre.name is null and cur.previous_run_started_at is not null
    ),

    models_removed as (
        select
            pre.node_id,
            pre.command_invocation_id,
            'model_removed' as change,
            null as column_name,
            null as data_type,
            null as pre_data_type,
            pre.next_run_started_at as detected_at
        from pre_models pre
        left outer join
            cur_models cur
            on pre.node_id = cur.node_id
                and pre.next_run_started_at = cur.run_started_at
        where cur.name is null
        and pre.next_run_started_at is not null
    ),

    seeds_added as (
        select
            cur.node_id,
            cur.command_invocation_id,
            'seed_added' as change,
            null as column_name,
            null as data_type,
            null as pre_data_type,
            cur.run_started_at as detected_at
        from cur_seeds cur
        left outer join
            pre_seeds pre
            on cur.node_id = pre.node_id
                and cur.previous_run_started_at = pre.run_started_at
        where pre.name is null and cur.previous_run_started_at is not null
    ),

    seeds_removed as (
        select
            pre.node_id,
            pre.command_invocation_id,
            'seed_removed' as change,
            null as column_name,
            null as data_type,
            null as pre_data_type,
            pre.next_run_started_at as detected_at
        from pre_seeds pre
        left outer join
            cur_seeds cur
            on pre.node_id = cur.node_id
                and pre.next_run_started_at = cur.run_started_at
        where cur.name is null
        and pre.next_run_started_at is not null
    ),

    sources_added as (
        select
            cur.node_id,
            cur.command_invocation_id,
            'source_added' as change,
            null as column_name,
            null as data_type,
            null as pre_data_type,
            cur.run_started_at as detected_at
        from cur_sources cur
        left outer join
            pre_sources pre
            on cur.node_id = pre.node_id
                and cur.previous_run_started_at = pre.run_started_at
        where pre.name is null and cur.previous_run_started_at is not null
    ),

    sources_removed as (
        select
            pre.node_id,
            pre.command_invocation_id,
            'source_removed' as change,
            null as column_name,
            null as data_type,
            null as pre_data_type,
            pre.next_run_started_at as detected_at
        from pre_sources pre
        left outer join
            cur_sources cur
            on pre.node_id = cur.node_id
                and pre.next_run_started_at = cur.run_started_at
        where cur.name is null
        and pre.next_run_started_at is not null
    ),

    snapshots_added as (
        select
            cur.node_id,
            cur.command_invocation_id,
            'snapshot_added' as change,
            null as column_name,
            null as data_type,
            null as pre_data_type,
            cur.run_started_at as detected_at
        from cur_snapshots cur
        left outer join
            pre_snapshots pre
            on cur.node_id = pre.node_id
                and cur.previous_run_started_at = pre.run_started_at
        where pre.name is null and cur.previous_run_started_at is not null
    ),

    snapshots_removed as (
        select
            pre.node_id,
            pre.command_invocation_id,
            'snapshot_removed' as change,
            null as column_name,
            null as data_type,
            null as pre_data_type,
            pre.next_run_started_at as detected_at
        from pre_snapshots pre
        left outer join
            cur_snapshots cur
            on pre.node_id = cur.node_id
                and pre.next_run_started_at = cur.run_started_at
        where cur.name is null
        and pre.next_run_started_at is not null
    ),

    columns_removed_filter_deleted_tables as (
        select
            removed.node_id,
            removed.command_invocation_id,
            removed.change,
            removed.column_name,
            removed.data_type,
            removed.pre_data_type,
            removed.detected_at
        from columns_removed as removed inner join cur
            on (lower(removed.node_id) = lower(cur.node_id))

    ),

    all_changes as (
        select
            node_id,
            command_invocation_id,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=1) }} as resource_type,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=2) }} as project,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=3) }} as resource_name,
            change,
            column_name,
            data_type,
            pre_data_type,
            detected_at
        from type_changes
        union all
        select
            node_id,
            command_invocation_id,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=1) }} as resource_type,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=2) }} as project,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=3) }} as resource_name,
            change,
            column_name,
            data_type,
            pre_data_type,
            detected_at
        from columns_removed_filter_deleted_tables
        union all
        select
            node_id,
            command_invocation_id,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=1) }} as resource_type,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=2) }} as project,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=3) }} as resource_name,
            change,
            column_name,
            data_type,
            pre_data_type,
            detected_at
        from columns_added
        union all
        select
            node_id,
            command_invocation_id,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=1) }} as resource_type,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=2) }} as project,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=3) }} as resource_name,
            change,
            column_name,
            data_type,
            pre_data_type,
            detected_at
        from models_added
        union all
        select
            node_id,
            command_invocation_id,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=1) }} as resource_type,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=2) }} as project,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=3) }} as resource_name,
            change,
            column_name,
            data_type,
            pre_data_type,
            detected_at
        from models_removed
        union all
        select
            node_id,
            command_invocation_id,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=1) }} as resource_type,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=2) }} as project,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=3) }} as resource_name,
            change,
            column_name,
            data_type,
            pre_data_type,
            detected_at
        from seeds_added
        union all
        select
            node_id,
            command_invocation_id,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=1) }} as resource_type,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=2) }} as project,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=3) }} as resource_name,
            change,
            column_name,
            data_type,
            pre_data_type,
            detected_at
        from seeds_removed
        union all
        select
            node_id,
            command_invocation_id,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=1) }} as resource_type,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=2) }} as project,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=3) }} as resource_name,
            change,
            column_name,
            data_type,
            pre_data_type,
            detected_at
        from sources_added
        union all
        select
            node_id,
            command_invocation_id,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=1) }} as resource_type,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=2) }} as project,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=3) }} as resource_name,
            change,
            column_name,
            data_type,
            pre_data_type,
            detected_at
        from sources_removed
        union all
        select
            node_id,
            command_invocation_id,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=1) }} as resource_type,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=2) }} as project,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=3) }} as resource_name,
            change,
            column_name,
            data_type,
            pre_data_type,
            detected_at
        from snapshots_added
        union all
        select
            node_id,
            command_invocation_id,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=1) }} as resource_type,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=2) }} as project,
            {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=3) }} as resource_name,
            change,
            column_name,
            data_type,
            pre_data_type,
            detected_at
        from snapshots_removed

    )

select distinct
    {{ dbt_utils.generate_surrogate_key(["command_invocation_id", "node_id"]) }} as execution_key,
    node_id,
    command_invocation_id,
    resource_type,
    project,
    resource_name,
    change,
    column_name,
    data_type,
    pre_data_type,
    detected_at
from all_changes
