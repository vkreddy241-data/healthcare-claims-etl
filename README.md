# Healthcare Claims ETL Pipeline

![CI](https://github.com/vkreddy241-data/healthcare-claims-etl/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![AWS Glue](https://img.shields.io/badge/AWS%20Glue-4.0-FF9900?logo=amazonaws)
![Redshift](https://img.shields.io/badge/Redshift-ra3-8C4FFF?logo=amazonredshift)
![dbt](https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt)
![Terraform](https://img.shields.io/badge/Terraform-1.5-7B42BC?logo=terraform)

HIPAA-compliant, daily ETL pipeline for healthcare insurance claims — extracts raw claims from S3, applies business rules and PII masking, loads to Redshift, and builds analytics-ready models with dbt.

---

## Architecture

```
S3 Raw Zone (CSV)
      │
      ▼
 AWS Glue: extract_claims.py
  - Schema validation
  - PII masking (SHA-256 member_id)
  - Parquet conversion
      │
      ▼
S3 Staging Zone (Parquet, partitioned by claim_type)
      │
      ▼
 AWS Glue: transform_claims.py
  - Reference data enrichment (members, providers)
  - Business rules (denied_flag, high_cost_flag, OON, LOS)
  - Deduplication (row_number over claim_id)
  - Delta Lake upsert (SCD Type 1 merge)
      │
      ▼
S3 Conformed Zone (Delta Lake)
      │
      ▼
 AWS Glue: load_redshift.py
  - Bulk COPY into Redshift fact_claims
      │
      ▼
Redshift (claims_dw)
      │
      ▼
dbt models: stg_claims → int_claims_enriched → mart_claims_summary
      │
      ▼
Power BI / Tableau Dashboards
```

## Key Features

| Feature | Detail |
|---|---|
| **HIPAA Compliance** | SHA-256 PII masking on member_id |
| **Data Volume** | Designed for 500K–2M claims/day |
| **SCD Handling** | Delta Lake merge for claim resubmissions |
| **Business Rules** | Denial rate, high-cost, OON, length-of-stay |
| **Data Quality** | dbt tests + Airflow DQ sensors |
| **Orchestration** | Airflow with retries, SLA monitoring, email alerts |
| **IaC** | Terraform manages Glue, Redshift, S3, IAM |

## Project Structure

```
healthcare-claims-etl/
├── glue_jobs/
│   ├── extract_claims.py       # S3 CSV → Parquet staging + PII mask
│   ├── transform_claims.py     # Business rules + Delta Lake upsert
│   └── load_redshift.py        # Delta → Redshift bulk load
├── dbt/
│   ├── models/
│   │   ├── staging/stg_claims.sql
│   │   ├── intermediate/int_claims_enriched.sql
│   │   └── marts/mart_claims_summary.sql
│   ├── tests/test_claims_integrity.yml
│   └── macros/pii_mask.sql
├── airflow/dags/
│   └── claims_etl_dag.py       # Daily orchestration DAG
├── infra/terraform/            # Glue + Redshift + S3 via Terraform
├── tests/
│   └── test_glue_jobs.py       # pytest unit tests (local Spark)
└── .github/workflows/ci.yml
```

## Running Locally

```bash
pip install -r requirements.txt
pytest tests/ -v
```

## Deploy

```bash
cd infra/terraform
terraform init
terraform apply -var="vpc_id=vpc-xxx" -var="redshift_password=<secret>"
```

## Tech Stack

**AWS:** Glue 4.0, Redshift ra3, S3, MWAA, Secrets Manager
**Processing:** PySpark 3.5, Delta Lake 3.0
**Transformation:** dbt-redshift 1.7
**Orchestration:** Apache Airflow 2.8
**IaC:** Terraform 1.5
**CI/CD:** GitHub Actions

---
Built by [Vikas Reddy Amaravathi](https://linkedin.com/in/vikas-reddy-a-avr03) — Azure Data Engineer @ Cigna
