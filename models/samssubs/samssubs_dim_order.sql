{{ config(
    materialized = 'table',
    schema = 'dw_samssubs') 
}}
SELECT 
{{ dbt_utils.generate_surrogate_key(['ordernumber']) }} AS order_key,
ordernumber,
ordermethod
FROM {{ source('samssubs_landing','"ORDER"') }}
ORDER BY 1