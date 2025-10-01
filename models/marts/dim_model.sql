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

    _tests as (
        select
            command_invocation_id,
            lower(
                {% if target.type in ('snowflake', 'bigquery') %}
                    array_to_string(depends_on_nodes, '')
                {% else %}
                depends_on_nodes
            {% endif %})
                as depends_on_nodes
        from {{ ref('dbt_observability_marts', 'stg_test') }}
    ),

    _models as (
        select
            node_id,
            command_invocation_id,
            model_key,
            {{ dbt.concat(["'%'", 'lower(node_id)', "'%'"]) }} as node_key
        from models
    ),

    untested_models as (
        select
            models.command_invocation_id,
            models.node_id
        from _models as models
        left outer join _tests as test
            on models.command_invocation_id = test.command_invocation_id
                and test.depends_on_nodes like models.node_key
        where test.depends_on_nodes is null
    ),

    final as (
        select
            models.model_key,
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
                        from untested_models
                        where models.node_id = untested_models.node_id
                            and models.command_invocation_id = untested_models.command_invocation_id
                    ) then 'No'
                else 'Yes'
            end as is_tested
        from models

    )

select * from final
