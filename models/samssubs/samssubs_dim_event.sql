{{ config(
    materialized = 'table',
    schema = 'dw_samssubs') 
}}
SELECT 
{{ dbt_utils.generate_surrogate_key(['event_name']) }} AS event_key,
event_name
FROM {{ source('samssubs_landing','WEB_TRAFFIC_EVENTS') }}
ORDER BY 1