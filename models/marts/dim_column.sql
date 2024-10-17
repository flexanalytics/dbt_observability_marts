{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
with
    columns as (
        select
            command_invocation_id,
            node_id,
            resource_type,
            project,
            resource_name,
            column_name,
            data_type,
            tags,
            meta,
            description
        from {{ ref('dbt_observability_marts', 'int_column') }}
    )

select distinct
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id', 'column_name']) }} as column_key,
    node_id,
    resource_type,
    project,
    resource_name,
    column_name,
    data_type,
    tags,
    meta,
    description
from columns
