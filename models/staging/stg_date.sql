{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
{% set end_date = run_started_at.strftime("%Y-%m-%d") %}

{{ dbt_date.get_date_dimension(var('start_date'), end_date) }}
