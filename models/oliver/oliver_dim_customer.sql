--dbt is case sensitive

{{ config(
    materialized = 'table',
    schema = 'DW_oliver') 
}}

SELECT 
CUSTOMER_ID AS CUSTOMER_KEY,
CUSTOMER_ID,
LAST_NAME,
FIRST_NAME,
STATE,
EMAIL,
PHONE_NUMBER
FROM {{ source('oliver_raw', 'CUSTOMER')}}
ORDER BY 1 ASC