{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
{% set ref_union = (var('dbt_observability:objects', none) is not none) %}
select
    command_invocation_id,
    unique_id,
    name,
    label,
    model,
    expression,
    calculation_method,
    filters,
    time_grains,
    dimensions,
    package_name,
    meta,
    depends_on_nodes,
    description
{% if ref_union %}
from {{ ref('dbt_observability_marts', 'metrics') }}
{% else %}
from {{ ref('dbt_observability', 'metrics') }}
{% endif %}

