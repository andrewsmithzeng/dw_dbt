{{ config(
    materialized = 'table',
    schema = 'dw_samssubs') 
}}
SELECT 
TO_DATE(w.EVENT_TIMESTAMP) AS DATE_KEY,
w.event_key,
w.source_key,
w.page_key,
COUNT (*) AS countofinteractions
FROM {{ source("samssubs_landing",'WEB_TRAFFIC_EVENTS') }} w
LEFT JOIN {{ ref("samssubs_dim_date") }} d ON TO_DATE(w.EVENT_TIMESTAMP) = d.date_key
LEFT JOIN {{ ref("samssubs_dim_event") }} e ON w.event_key = e.event_key
LEFT JOIN {{ ref("samssubs_dim_source") }} s ON w.source_key = s.source_key
LEFT JOIN {{ ref("samssubs_dim_page") }} p ON w.page_key = p.page_key
GROUP BY w.TO_DATE(EVENT_TIMESTAMP),
w.event_key,
w.source_key,
w.page_key
ORDER BY 1
