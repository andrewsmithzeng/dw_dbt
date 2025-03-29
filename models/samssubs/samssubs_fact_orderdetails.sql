{{ config(
    materialized = 'table',
    schema = 'dw_samssubs') 
}}
SELECT 
od.date_key,
od.order_key,
od.product_key,
od.store_key,
od.employee_key,
od.Cust_Key,
orderlineqty,
orderlineprice,
pointsearned
FROM {{ source("samssubs_landing",'ORDERDETAILS') }} od 
LEFT JOIN {{ ref("samssubs_dim_date") }} d ON od.date_key = d.date_key
LEFT JOIN {{ ref('samssubs_dim_order') }} o ON od.order_key = o.order_key
LEFT JOIN {{ ref("samssubs_dim_product") }} p ON od.product_key = p.product_key
LEFT JOIN {{ ref("samssubs_dim_store") }} s ON od.store_key = s.store_key
LEFT JOIN {{ ref("samssubs_dim_employee") }} e ON od.employee_key = e.employee_key
LEFT JOIN {{ ref("samssubs_dim_customer") }} c ON od.cust_key = c.cust_key
ORDER BY 1