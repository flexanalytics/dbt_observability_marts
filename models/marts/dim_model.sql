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
                when exists (
                        select 1
                        from {{ ref('dbt_observability_marts', 'int_test_model') }} as test_model
                        where test_model.model_key = models.model_key
                    ) then 'Yes'
                else 'No'
            end as is_tested
        from models

    )

select * from final
