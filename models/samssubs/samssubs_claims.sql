{{ config(
    materialized = 'table',
    schema = 'dw_samssubs') 
}}
with semantic_orderdetails  as (
  select 
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
    c.CustomerFName || ' ' || c.CustomerLName as customer_name,
    
  from {{ ref('samssubs_fact_orderdetails') }} od
  left join {{ ref('samssubs_dim_product') }} p on od.Product_Key = p.Product_Key
  left join {{ ref('samssubs_dim_store') }} s on od.Store_Key = s.Store_Key
  left join {{ ref('samssubs_dim_customer') }} c on od.Cust_Key = c.Cust_Key
  left join {{ ref('samssubs_dim_date') }} d on od.Date_Key = d.Date_Key

  GROUP BY od.Date_Key,od.order_key,od.PointsEarned,od.product_key,od.product_key,p.ProductName,od.employee_key,
  e.employeefname, e.employeelname,od.cust_key,c.CustomerFName,c.CustomerLName
)
  

semantic_webtracking as (
  select 
    w.Date_Key as date
    e.Event_name,
    s.SourceName,
    p.page_url,
    w.CountOfInteractions
  from {{ ref('samssubs_fact_webtracking') }} w
  left join {{ ref('samssubs_dim_event') }} e on w.Event_Key = e.Event_Key
  left join {{ ref('samssubs_dim_source') }} s on w.Source_Key = s.Source_Key
  left join {{ ref('samssubs_dim_page') }} p on w.Page_Key = p.Page_Key
  left join {{ ref('samssubs_dim_date') }} d on w.Date_Key = d.Date_Key
)

select *
from semantic_orderdetails so
full outer join semantic_webtracking sw
on so.date = sw.date
