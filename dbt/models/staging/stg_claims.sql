-- stg_claims: Light cleansing on top of raw Redshift landing table.
-- Renames columns to snake_case, casts types, filters obvious garbage rows.

{{
  config(
    materialized = 'view',
    schema       = 'staging'
  )
}}

with source as (
    select * from {{ source('claims_raw', 'fact_claims') }}
),

renamed as (
    select
        claim_id                                    as claim_id,
        member_id                                   as member_id,
        provider_npi                                as provider_npi,
        cast(service_date   as date)                as service_date,
        cast(discharge_date as date)                as discharge_date,
        upper(trim(diagnosis_code))                 as diagnosis_code,
        upper(trim(procedure_code))                 as procedure_code,
        cast(billed_amount  as numeric(18, 2))      as billed_amount,
        cast(allowed_amount as numeric(18, 2))      as allowed_amount,
        cast(paid_amount    as numeric(18, 2))      as paid_amount,
        upper(trim(claim_status))                   as claim_status,
        lower(trim(claim_type))                     as claim_type,
        plan_id,
        denied_flag,
        high_cost_flag,
        out_of_network_flag,
        length_of_stay_days,
        cast(cost_ratio     as numeric(8, 4))       as cost_ratio,
        service_year,
        service_month,
        provider_name,
        specialty,
        network_flag,
        state,
        ingested_at,
        run_date

    from source

    where claim_id    is not null
      and member_id   is not null
      and billed_amount >= 0
      and service_date  is not null
)

select * from renamed
