{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
with
    models as (
        select
            {{ dbt_utils.generate_surrogate_key([
                'command_invocation_id', 'node_id'
                ]) }} as model_key,
            {{ dbt_utils.generate_surrogate_key([
                'command_invocation_id'
                ]) }} as invocation_key,
            command_invocation_id,
            node_id,
            run_started_at,
            resource_type,
            project,
            resource_name,
            database_name,
            schema_name,
            name,
            package_name,
            path,
            checksum,
            materialization,
            tags,
            meta,
            description
        from {{ ref('dbt_observability_marts', 'int_model') }}
    ),

    tested_models as (
        select distinct model_key
        from {{ ref('dbt_observability_marts', 'int_test_model') }}
        where model_key is not null
    ),

    final as (
        select
            models.model_key,
            models.invocation_key,
            models.node_id,
            models.resource_type,
            models.project,
            models.resource_name,
            models.database_name,
            models.schema_name,
            models.name,
            models.package_name,
            models.path,
            models.checksum,
            models.materialization,
            models.tags,
            models.meta,
            models.description,
            case
                when tested_models.model_key is not null then 'Yes'
                else 'No'
            end as is_tested
        from models
        left outer join tested_models
            on models.model_key = tested_models.model_key
    )

select * from final
