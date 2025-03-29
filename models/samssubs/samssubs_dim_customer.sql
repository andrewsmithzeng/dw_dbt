{{ config(
    materialized = 'table',
    schema = 'dw_samssubs') 
}}

SELECT 
CUSTOMERID AS Cust_Key,
CustomerID,
customerlname,
customerfname,
customerbday,
customerphone
FROM {{ source('samssubs_landing', 'CUSTOMER')}}
ORDER BY 1 ASC