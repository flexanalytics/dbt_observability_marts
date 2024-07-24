{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
{% set ref_union = (var('dbt_observability:objects', none) is not none) %}
select
    command_invocation_id,
    node_id,
    run_started_at,
    database_name,
    schema_name,
    name,
    package_name,
    path,
    checksum
{% if ref_union %}
from {{ ref('dbt_observability_marts', 'seeds') }}
{% else %}
from {{ ref('dbt_observability', 'seeds') }}
{% endif %}

