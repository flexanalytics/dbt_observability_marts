{{
    config(
        alias='dim_source',
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
    from {{ ref('dbt_observability_marts', 'observability__int_source') }}
),

tested_sources as (
    select distinct source_key
    from {{ ref('dbt_observability_marts', 'observability__int_test_model') }}
    where source_key is not null
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
        when tested_sources.source_key is not null then 'Yes'
        else 'No'
    end as is_tested
from source
left outer join tested_sources
    on source.source_key = tested_sources.source_key
