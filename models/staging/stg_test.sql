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
    description,
    depends_on_nodes,
    package_name,
    test_path,
    tags,
    test_metadata
{% if ref_union %}
from {{ ref('dbt_observability_marts', 'tests') }}
{% else %}
from {{ ref('dbt_observability', 'tests') }}
{% endif %}

