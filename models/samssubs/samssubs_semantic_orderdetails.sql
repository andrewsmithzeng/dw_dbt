{{ config(materialized='table', schema='dw_samssubs') }}
SELECT 
    od.Date_Key AS date,
    od.order_key AS orderid,
    o.ordermethod,
    od.product_key AS productid,
    p.productname,
    p.producttype,
    p.length,
    p.productcost,
    od.orderlineqty AS productqty,
    od.PointsEarned,
    od.employee_key AS employeeid,
    e.employeefname || ' ' || e.employeelname AS employee_name,
    od.cust_key AS customerid,
    c.CustomerFName || ' ' || c.CustomerLName as customer_name
FROM {{ ref('samssubs_fact_orderdetails') }} od
LEFT JOIN {{ ref('samssubs_dim_product') }} p ON od.Product_Key = p.Product_Key
LEFT JOIN {{ ref("samssubs_dim_employee") }} e ON od.employee_key = e.employee_key
LEFT JOIN {{ ref('samssubs_dim_customer') }} c ON od.Cust_Key = c.Cust_Key
LEFT JOIN {{ ref('samssubs_dim_store') }} s on od.store_key = s.store_key
LEFT JOIN {{ ref('samssubs_dim_order') }} o on od.order_key = o.order_key
--GROUP BY od.Date_Key, od.order_key, od.PointsEarned, od.product_key, p.ProductType, p.ProductName, od.employee_key, e.employeefname, e.employeelname, od.cust_key, c.CustomerFName, c.CustomerLName
GROUP BY ALL