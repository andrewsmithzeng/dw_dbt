{{ config(
    materialized = 'table',
    schema = 'dw_insurance'
    )
}}


select
policyid as policy_key,
policyid,
policytype
FROM {{ source('insurance_landing', 'policies') }}