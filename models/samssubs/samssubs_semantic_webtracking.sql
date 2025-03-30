{{ config(materialized='table', schema='dw_samssubs') }}
SELECT 
    w.Date_Key as date,
    e.Event_name,
    s.Source_Name,
    p.page_url,
    w.CountOfInteractions
FROM {{ ref('samssubs_fact_webtracking') }} w
LEFT JOIN {{ ref('samssubs_dim_event') }} e ON w.Event_Key = e.Event_Key
LEFT JOIN {{ ref('samssubs_dim_source') }} s ON w.Source_Key = s.Source_Key
LEFT JOIN {{ ref('samssubs_dim_page') }} p ON w.Page_Key = p.Page_Key
