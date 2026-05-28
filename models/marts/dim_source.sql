{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
with
source as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'command_invocation_id',
            'node_id'
        ]) }} as source_key,
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
        freshness
    from {{ ref('dbt_observability_marts', 'int_source') }}
)

select
    source.source_key,
    node_id,
    resource_type,
    project,
    resource_name,
    database_name,
    schema_name,
    source_name,
    loader,
    name,
    identifier,
    loaded_at_field,
    freshness,
    case
        when exists (
                select 1
                from {{ ref('dbt_observability_marts', 'int_test_model') }} as test_model
                where test_model.source_key = source.source_key
            ) then 'Yes'
        else 'No'
    end as is_tested
from source
