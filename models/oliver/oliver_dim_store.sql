 {{ config
    (materialized = 'table',
    schema = 'dw_oliver')
 }}

 SELECT 
 store_id AS store_key,
 store_id,
 store_name,
 street,
 city,
 State
 FROM {{ source('oliver_raw', 'STORE') }}
 ORDER BY 1