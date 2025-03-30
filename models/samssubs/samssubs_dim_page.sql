{{ config(
    materialized = 'table',
    schema = 'dw_samssubs') 
}}
SELECT DISTINCT
{{ dbt_utils.generate_surrogate_key(['page_url']) }} AS page_key,
page_url
FROM {{ source('samssubs_landing','WEB_TRAFFIC_EVENTS') }}
ORDER BY 1