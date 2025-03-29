{{ config(
    materialized = 'table',
    schema = 'dw_samssubs') 
}}
SELECT p.productid AS product_key,
p.productid,
producttype,
productname,
productcost,
productcalories,
length,
breadtype
FROM {{ source("samssubs_landing","PRODUCT") }} p JOIN 
{{ source("samssubs_landing",'SANDWICH') }} s on p.productid = s.productid
ORDER BY 1