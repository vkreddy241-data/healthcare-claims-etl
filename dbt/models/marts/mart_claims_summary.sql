-- mart_claims_summary: Monthly claims KPIs by plan and claim type.
-- Powers Power BI / Tableau dashboards used by the actuarial team.

{{
  config(
    materialized = 'table',
    schema       = 'marts',
    sort          = ['service_year', 'service_month'],
    dist          = 'plan_id'
  )
}}

with base as (
    select * from {{ ref('int_claims_enriched') }}
),

summary as (
    select
        service_year,
        service_month,
        plan_id,
        claim_type,
        claim_category,
        state,

        count(distinct claim_id)                                as total_claims,
        count(distinct member_id)                               as unique_members,
        count(distinct provider_npi)                            as unique_providers,

        sum(billed_amount)                                      as total_billed,
        sum(allowed_amount)                                     as total_allowed,
        sum(paid_amount)                                        as total_paid,
        avg(paid_amount)                                        as avg_paid_per_claim,
        median(paid_amount)                                     as median_paid_per_claim,

        sum(case when denied_flag        then 1 else 0 end)     as denied_claims,
        sum(case when high_cost_flag     then 1 else 0 end)     as high_cost_claims,
        sum(case when out_of_network_flag then 1 else 0 end)    as out_of_network_claims,

        round(
            sum(case when denied_flag then 1 else 0 end)::float
            / nullif(count(distinct claim_id), 0) * 100, 2
        )                                                       as denial_rate_pct,

        avg(length_of_stay_days)                                as avg_los_days,
        avg(cost_ratio)                                         as avg_cost_ratio,
        avg(risk_score)                                         as avg_member_risk_score,

        current_timestamp                                       as dbt_updated_at

    from base
    group by 1, 2, 3, 4, 5, 6
)

select * from summary
