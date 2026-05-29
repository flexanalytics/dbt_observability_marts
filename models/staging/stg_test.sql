{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
{% set ref_union = (var('dbt_observability:objects', none) is not none) %}
{% if ref_union %}
    {% set source_relation = ref('dbt_observability_marts', 'tests') %}
{% else %}
    {% set source_relation = ref('dbt_observability', 'tests') %}
{% endif %}
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
    test_metadata,
    -- column_name / attached_node were added to upstream dbt_observability later
    -- than the other columns; tolerate their absence in older observability data
    {{ optional_column(source_relation, 'column_name') }},
    {{ optional_column(source_relation, 'attached_node') }}
from {{ source_relation }}
