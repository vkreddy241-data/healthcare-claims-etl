# Architecture — Healthcare Claims ETL

## Overview

A HIPAA-compliant batch ETL pipeline that extracts raw CMS-formatted claims files from S3, transforms them through AWS Glue PySpark jobs, and loads analytics-ready data into Amazon Redshift via dbt models.

```
S3 (raw claims files — CMS 837/835 format)
        │
        ▼
AWS Glue Job: extract_claims.py
  - Reads raw claims from S3 (CSV / Parquet)
  - Applies PII masking: member_id → SHA-256 hash, provider_npi → tokenised
  - Validates required fields (claim_id, service_date, billed_amount)
  - Writes to S3 staging zone (Parquet, partitioned by service_month)
        │
        ▼
AWS Glue Job: transform_claims.py
  - Standardises claim types (MEDICAL, PHARMACY, DENTAL, VISION, BEHAVIORAL)
  - Parses ICD-10 diagnosis codes, CPT procedure codes into arrays
  - Calculates derived metrics: allowed_ratio, denial_flag, days_to_payment
  - Applies SCD Type 2 merge logic for member dimension
  - Writes transformed claims to Delta Lake on S3
        │
        ▼
AWS Glue Job: load_redshift.py
  - Loads transformed Delta data into Redshift Staging schema
  - Uses COPY command via S3 manifest for bulk load performance
        │
        ▼
dbt (runs against Redshift)
  staging/stg_claims.sql          ← casts and renames raw columns
  intermediate/int_claims_enriched.sql  ← joins with member/provider dimensions
  marts/mart_claims_summary.sql   ← payer × claim_type × month aggregations

  macros/pii_mask.sql             ← reusable PII masking macro
  tests/test_claims_integrity.yml ← not_null, unique, accepted_values tests
        │
        ▼
Redshift (consumption)
  Analytics, BI tools, actuarial models
```

## Key Design Decisions

**Why PII masking at the Glue extraction layer (not dbt)?**  
Masking at extraction ensures PHI never lands in the dbt development environment or Redshift staging tables in clear text. Any engineer running dbt locally works with already-masked data, reducing the blast radius of credential exposure.

**Why AWS Glue over EMR or self-managed Spark?**  
Glue is serverless — no cluster management, and jobs scale automatically with file volume. For a batch ETL workload with predictable daily cadence, Glue's DPU-based billing is more cost-effective than a persistent EMR cluster.

**Why dbt on top of Redshift Staging instead of transforming everything in Glue?**  
Glue jobs are good at large-scale data movement and PySpark transformations. dbt is better at SQL-based business logic, documentation, testing, and lineage. The split keeps heavy lifting in Glue and business modeling in dbt.

**Why SCD Type 2 for the member dimension?**  
Healthcare members change plans, demographics, and coverage over time. SCD2 preserves the full history so claims can always be joined to the member attributes that were valid at service date — critical for actuarial accuracy.

## Pipeline Schedule (Airflow)

| Task | Schedule | SLA |
|---|---|---|
| extract_claims | Daily 02:00 UTC | 30 min |
| transform_claims | Daily 03:00 UTC | 45 min |
| load_redshift | Daily 04:00 UTC | 20 min |
| dbt run + test | Daily 05:00 UTC | 30 min |

## Tech Stack

| Layer | Technology |
|---|---|
| Storage | Amazon S3 (raw + staging + Delta Lake) |
| Processing | AWS Glue (PySpark) |
| Transformation | dbt (SQL, running on Redshift) |
| Data warehouse | Amazon Redshift |
| Orchestration | Apache Airflow |
| Infrastructure | Terraform (Glue jobs, S3 buckets, Redshift cluster, IAM) |

## HIPAA Controls

- PII masked via SHA-256 (member_id) and tokenisation (provider_npi) before any downstream storage
- Glue job IAM roles follow least-privilege (read-only S3 source, write-only S3 staging)
- Redshift cluster is VPC-only with no public endpoint
- dbt `pii_mask` macro enforces consistent masking across all models
