{{ config(
    materialized = 'table',
    schema = 'dw_samssubs') 
}}
SELECT DISTINCT
{{ dbt_utils.generate_surrogate_key(['traffic_source']) }} AS source_key,
traffic_source AS source_name
FROM {{ source('samssubs_landing','WEB_TRAFFIC_EVENTS') }}
ORDER BY 1