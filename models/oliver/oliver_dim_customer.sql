--dbt is case sensitive

{{ config(
    materialized = 'table',
    schema = 'DW_oliver') 
}}

SELECT 
CUSTOMER_ID AS Cust_Key,
Customer_ID,
FIRST_NAME AS FirstName,
LAST_NAME AS LastName,
Email,
PHONE_NUMBER AS PhoneNumber,
State
FROM {{ source('oliver_raw', 'CUSTOMER')}}
ORDER BY 1 ASC