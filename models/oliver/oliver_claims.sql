{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
) }}

SELECT 
f.cust_key,
c.firstname AS customer_first_name,
c.lastname AS customer_last_name,
f.date_key,
f.store_key,
f.product_key,
f.employee_key,
e.firstname AS employee_fist_name,
e.lastname AS employee_last_name,
quantity,
dollars_sold,
unit_price
FROM {{ ref('oliver_fact_sales') }} AS f
LEFT JOIN {{ ref("oliver_dim_customer") }} AS c ON f.cust_key = c.cust_key
LEFT JOIN {{ ref('oliver_dim_date') }} AS d ON f.date_key= d.date_key
LEFT JOIN {{ ref('oliver_dim_employee') }} AS e ON f.employee_key = e.employee_key
LEFT JOIN {{ ref('oliver_dim_product') }} AS p ON f.product_key = p.product_key
LEFT JOIN {{ ref("oliver_dim_store") }} AS s on f.store_key = s.store_key
ORDER BY 1