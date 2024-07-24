{% set build_union = (var('dbt_observability:objects', none) is not none) %}
{{
    config(
        enabled=build_union
    )
}}
{% set relations = [] %}
{% for database in var('dbt_observability:objects', '') %}
{% for schema in var('dbt_observability:objects', '')[database] %}

{% do relations.append(api.Relation.create(database=database, schema=schema, identifier='sources')) %}
{% endfor %}
{% endfor %}
{{ dbt_utils.union_relations(
    relations=relations
) }}
