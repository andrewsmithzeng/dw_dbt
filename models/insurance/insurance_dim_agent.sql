{{ config(
    materialized = 'table',
    schema = 'dw_insurance'
    )
}}

SELECT
agentid as agent_key,
agentid,
firstname,
lastname,
email,
phone
FROM {{ source('insurance_landing', 'agents') }}