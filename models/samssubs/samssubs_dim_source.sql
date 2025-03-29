{{ config(
    materialized = 'table',
    schema = 'dw_samssubs') 
}}
SELECT 
{{ dbt_utils.generate_surrogate_key(['traffic_source']) }} AS source_key,
traffic_source AS sourcename
FROM {{ source('samssubs_landing','WEB_TRAFFIC_EVENTS') }}
ORDER BY 1