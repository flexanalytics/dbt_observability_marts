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
    name,
    type,
    owner,
    maturity,
    path,
    description,
    url,
    package_name,
    depends_on_nodes
{% if ref_union %}
from {{ ref('dbt_observability_marts', 'exposures') }}
{% else %}
from {{ ref('dbt_observability', 'exposures') }}
{% endif %}

