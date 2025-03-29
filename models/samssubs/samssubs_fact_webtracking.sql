{{ config(
    materialized = 'table',
    schema = 'dw_samssubs') 
}}
SELECT 
d.date_key,
e.event_key,
s.source_key,
p.page_key,
COUNT (*) AS countofinteractions
FROM {{ source("samssubs_landing",'WEB_TRAFFIC_EVENTS') }} w
LEFT JOIN {{ ref("samssubs_dim_date") }} d ON TO_DATE(w.EVENT_TIMESTAMP) = d.date_id
LEFT JOIN {{ ref("samssubs_dim_event") }} e ON w.event_name = e.event_name
LEFT JOIN {{ ref("samssubs_dim_source") }} s ON w.traffic_source = s.source_name
LEFT JOIN {{ ref("samssubs_dim_page") }} p ON w.page_url = p.page_url
GROUP BY d.date_key,
e.event_key,
s.source_key,
p.page_key
ORDER BY 1
