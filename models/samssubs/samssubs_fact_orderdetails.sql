{{ config(
    materialized = 'table',
    schema = 'dw_samssubs'
) }}

SELECT 
    d.date_key,
    o.order_key,
    p.product_key,
    s.store_key,
    e.employee_key,
    c.cust_key,
    od.orderlineqty,
    od.orderlineprice,
    od.pointsearned
FROM {{ source("samssubs_landing", "ORDERDETAILS") }} od 
INNER JOIN {{ source("samssubs_landing", '"ORDER"') }} ord 
    ON od.ordernumber = ord.ordernumber
LEFT JOIN {{ source("samssubs_landing", "EMPLOYEE") }} em 
    ON ord.employeeid = em.employeeid
LEFT JOIN {{ ref("samssubs_dim_date") }} d 
    ON TO_DATE(ord.orderdate) = d.date_key
LEFT JOIN {{ ref("samssubs_dim_order") }} o 
    ON od.ordernumber = o.ordernumber
LEFT JOIN {{ ref("samssubs_dim_product") }} p 
    ON od.productid = p.productid
LEFT JOIN {{ ref("samssubs_dim_store") }} s 
    ON em.storeid = s.storeid
LEFT JOIN {{ ref("samssubs_dim_employee") }} e 
    ON ord.employeeid = e.employeeid
LEFT JOIN {{ ref("samssubs_dim_customer") }} c 
    ON ord.customerid = c.customerid
ORDER BY d.date_key
