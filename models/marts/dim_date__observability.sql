{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
with
    dt as (
        select
            *,
            case
                when month_of_year >= 7
                    then year_number + 1
                else year_number
            end as fiscal_year,
            case
                when month_of_year >= 7
                    then quarter_of_year - 2
                else quarter_of_year + 2
            end as fiscal_quarter,
            case
                when month_of_year >= 7
                    then month_of_year - 6
                else month_of_year + 6
            end as fiscal_month
        from {{ ref('dbt_observability_marts', 'stg_date') }}
    )

select
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key,
    date_day as date_full,
	{{ concat([
	    "fiscal_month",
		"' - '",
		"month_name"])
	}} as fiscal_month_name,
	{{ concat([
	    "fiscal_month",
		"' - '",
		"month_name_short"])
	}} as fiscal_month_abbrev,
    year_number as calendar_year,
    quarter_of_year as calendar_quarter,
    month_of_year as calendar_month_nbr,
    month_name as calendar_month_name,
    month_name_short as calendar_month_abbrev,
    week_of_year as calendar_week_of_year,
    cast(day_of_year as int) as calendar_day_of_year,
    cast(day_of_month as int) as day_of_month,
    day_of_week,
    day_of_week_name_short as day_of_week_abbrev,
    right({{ dbt.cast('date_day', api.Column.translate_type('string')) }}, 5) as month_day_num,
    {{ concat(["month_name_short", "' '", "day_of_month"]) }} as month_day_desc
from dt
