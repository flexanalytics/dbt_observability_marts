{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
select
    command_invocation_id,
    node_id,  -- resource_type.project.source_definition_name.table
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=1) }}
        as resource_type,
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=2) }}
        as project,
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=4) }}
        as resource_name,
    run_started_at,
    database_name,
    schema_name,
    source_name,
    loader,
    name,
    identifier,
    loaded_at_field,
    freshness,
    total_rowcount
from {{ ref('stg_source') }}
