{{ config(
    materialized = 'table',
    schema = 'DW_oliver'
    )
}}

with cte_date as (
{{ dbt_date.get_date_dimension("1990-01-01", "2050-12-31") }}
)

SELECT
date_day as date_key,
date_day as date_id,
day_of_week as dayofweek,
month_name as month,
quarter_of_year as quarter, 
year_number as year
from cte_date