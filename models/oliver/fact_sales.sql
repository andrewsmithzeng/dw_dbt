{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
) }}

SELECT 
cust_key,
date_key,
store_key,
product_key,
employee_key,
quantity,
total_amount AS dollars_sold,
unit_price
FROM {{ source('oliver_raw', 'ORDERS') }} AS o INNER JOIN {{ source('oliver_raw', 'ORDERLINE') }} ol
ON o.order_id = ol.order_id
LEFT JOIN {{ ref("oliver_dim_customer") }} AS c ON o.customer_id = c.cust_key
LEFT JOIN {{ ref('oliver_dim_date') }} AS d ON o.order_date= d.date_key
LEFT JOIN {{ ref('oliver_dim_employee') }} AS e ON o.employee_id = e.employee_key
LEFT JOIN {{ ref('oliver_dim_product') }} AS p ON ol.product_id = p.product_key
LEFT JOIN {{ ref("oliver_dim_store") }} AS s on o.store_id = s.store_key
ORDER BY 1