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
    depends_on_nodes,
    package_name,
    path,
    checksum,
    materialization,
    tags,
    meta,
    description,
    total_rowcount
{% if ref_union %}
from {{ ref('dbt_observability_marts', 'models') }}
{% else %}
from {{ ref('dbt_observability', 'models') }}
{% endif %}

