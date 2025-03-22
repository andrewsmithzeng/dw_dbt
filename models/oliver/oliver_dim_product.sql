{{ config 
(materialized = 'table',
schema = 'dw_oliver')
 }}

 SELECT
 product_id AS product_key,
 product_id,
 product_name,
 description
 FROM {{ source('oliver_raw', 'PRODUCT')}}
 ORDER BY 1