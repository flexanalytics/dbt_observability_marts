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
            description,
            is_documented,
            row_count,
            row_distinct,
            row_null,
            row_min,
            row_max,
            row_avg,
            row_sum,
            row_stdev,
            is_metric,
            column_values
        from {{ ref('dbt_observability_marts', 'int_column') }}
    )

select distinct
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id', 'column_name']) }} as column_key,
    {{ dbt_utils.generate_surrogate_key(['command_invocation_id', 'node_id']) }} as execution_key,
    is_documented,
    row_count,
    row_distinct,
    row_null,
    row_min,
    row_max,
    row_avg,
    row_sum,
    row_stdev,
    is_metric,
    column_values
from columns
