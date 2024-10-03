{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}

{% if (var('dbt_observability:objects', none) is not none) %}
    {% set target_name = 'dbt_observability_marts' %}
{% else %}
    {% set target_name = 'dbt_observability' %}
{% endif %}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref(target_name, 'sources')) %}

select
    command_invocation_id,
    node_id,
    run_started_at,
    database_name,
    schema_name,
    source_name,
    loader,
    name,
    identifier,
    loaded_at_field,
    freshness,
    {# total_rowcount is a new feature, so we check its existence before selecting it for backwards compatibility #}
    {% if 'total_rowcount' not in column_names | lower %}
    0 as
    {% endif %}
    total_rowcount
from {{ ref(target_name, 'sources') }}
where exists (
    select *
    from {{ ref(target_name, 'all_executions') }}
    where all_executions.command_invocation_id = sources.command_invocation_id
)
