{{ config(
    materialized = 'table',
    schema = 'dw_samssubs') 
}}
SELECT 
storeid AS store_key,
storeid,
address,
city,
state,
zip
FROM {{ source("samssubs_landing",'STORE') }}
ORDER BY 1