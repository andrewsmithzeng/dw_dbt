{{ config(materialized='table', schema='dw_samssubs') }}
SELECT 
    od.Date_Key AS date,
    od.order_key AS orderid,
    SUM(od.OrderLineQty * od.OrderLinePrice) AS total_sales,
    od.PointsEarned,
    od.product_key AS productid,
    p.ProductType,
    p.ProductName,
    od.employee_key AS employeeid,
    e.employeefname || ' ' || e.employeelname AS employee_name,
    od.cust_key AS customerid,
    c.CustomerFName || ' ' || c.CustomerLName as customer_name
FROM {{ ref('samssubs_fact_orderdetails') }} od
LEFT JOIN {{ ref('samssubs_dim_product') }} p ON od.Product_Key = p.Product_Key
LEFT JOIN {{ ref("samssubs_dim_employee") }} e ON od.employee_key = e.employee_key
LEFT JOIN {{ ref('samssubs_dim_customer') }} c ON od.Cust_Key = c.Cust_Key
GROUP BY od.Date_Key, od.order_key, od.PointsEarned, od.product_key, p.ProductType, p.ProductName, od.employee_key, e.employeefname, e.employeelname, od.cust_key, c.CustomerFName, c.CustomerLName
