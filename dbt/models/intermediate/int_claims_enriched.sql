-- int_claims_enriched: Joins staging claims with member and provider dimensions.
-- Adds derived flags used in downstream marts.

{{
  config(
    materialized = 'incremental',
    schema       = 'intermediate',
    unique_key   = 'claim_id',
    incremental_strategy = 'merge'
  )
}}

with claims as (
    select * from {{ ref('stg_claims') }}
    {% if is_incremental() %}
        where run_date > (select max(run_date) from {{ this }})
    {% endif %}
),

members as (
    select * from {{ ref('dim_members') }}
),

providers as (
    select * from {{ ref('dim_providers') }}
),

enriched as (
    select
        c.*,

        -- Member attributes
        m.age_band,
        m.gender,
        m.chronic_condition_flag,
        m.risk_score,

        -- Provider attributes
        p.provider_tier,
        p.hospital_flag,
        p.teaching_hospital_flag,

        -- Derived
        case
            when c.claim_type = 'medical'   and c.high_cost_flag  then 'high_cost_medical'
            when c.claim_type = 'pharmacy'                         then 'pharmacy'
            when c.out_of_network_flag                             then 'out_of_network'
            else 'standard'
        end                                                         as claim_category,

        datediff('day', c.service_date, current_date)              as days_since_service

    from claims        c
    left join members  m on c.member_id    = m.member_id
    left join providers p on c.provider_npi = p.provider_npi
)

select * from enriched
