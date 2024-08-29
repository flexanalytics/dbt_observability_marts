{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
select
    command_invocation_id,
    node_id,
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=1) }} as resource_type,
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=2) }} as project,
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=3) }} as resource_name,
    run_started_at,
    was_full_refresh,
    thread_id,
    status,
    compile_started_at,
    query_completed_at,
    total_node_runtime,
    materialization,
    schema_name
from {{ ref('stg_execution') }}
union
select
    command_invocation_id,
    node_id,
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=1) }} as resource_type,
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=2) }} as project,
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=4) }} as resource_name,
    run_started_at,
    null as was_full_refresh,
    null as thread_id,
    null as status,
    run_started_at as compile_started_at,
    run_started_at as query_completed_at,
    0 as total_node_runtime,
    'source' as materialization,
    schema_name
from {{ ref('stg_source') }}
