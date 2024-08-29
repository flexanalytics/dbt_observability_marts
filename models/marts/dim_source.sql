{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
with
source as (
    select
        command_invocation_id,
        node_id,
        resource_type,
        project,
        resource_name,
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
    from {{ ref('int_source') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'command_invocation_id',
        'node_id'
    ]) }} as source_key,
    node_id,
    database_name,
    schema_name,
    source_name,
    loader,
    name,
    identifier,
    loaded_at_field,
    freshness,
    total_rowcount,
    lag(total_rowcount)
        over (partition by node_id order by run_started_at)
        as previous_rowcount,
    avg(total_rowcount) over (partition by node_id) as average_rowcount
from source
