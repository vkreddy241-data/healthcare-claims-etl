"""
AWS Glue Job: Extract raw healthcare claims from S3 (CSV/JSON)
and write to S3 staging zone as Parquet with schema validation.

Trigger: Daily at 01:00 UTC via Airflow
Input:   s3://vkreddy-claims-raw/incoming/YYYY/MM/DD/
Output:  s3://vkreddy-claims-lake/staging/claims/
"""

import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "run_date", "env"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RUN_DATE = args["run_date"]          # e.g. 2024-06-01
ENV      = args["env"]               # dev / prod
RAW_PATH     = f"s3://vkreddy-claims-raw/incoming/{RUN_DATE.replace('-', '/')}/"
STAGING_PATH = f"s3://vkreddy-claims-lake/staging/claims/run_date={RUN_DATE}/"

# ---------------------------------------------------------------------------
# Expected schema for raw claims CSV
# ---------------------------------------------------------------------------
CLAIMS_SCHEMA = StructType([
    StructField("claim_id",          StringType(),  False),
    StructField("member_id",         StringType(),  False),
    StructField("provider_npi",      StringType(),  True),
    StructField("service_date",      StringType(),  True),
    StructField("discharge_date",    StringType(),  True),
    StructField("diagnosis_code",    StringType(),  True),
    StructField("procedure_code",    StringType(),  True),
    StructField("billed_amount",     DoubleType(),  True),
    StructField("allowed_amount",    DoubleType(),  True),
    StructField("paid_amount",       DoubleType(),  True),
    StructField("claim_status",      StringType(),  True),
    StructField("plan_id",           StringType(),  True),
    StructField("claim_type",        StringType(),  True),   # medical/pharmacy/dental
])


def read_raw(path: str):
    return spark.read.schema(CLAIMS_SCHEMA).option("header", True).csv(path)


def validate_and_clean(df):
    total = df.count()

    # Drop rows missing critical keys
    df = df.dropna(subset=["claim_id", "member_id"])

    # Enforce non-negative amounts
    df = df.filter(
        (F.col("billed_amount") >= 0) &
        (F.col("paid_amount") >= 0)
    )

    # Standardise dates
    df = (
        df
        .withColumn("service_date",   F.to_date("service_date",   "yyyy-MM-dd"))
        .withColumn("discharge_date", F.to_date("discharge_date", "yyyy-MM-dd"))
    )

    # Add audit columns
    df = (
        df
        .withColumn("run_date",    F.lit(RUN_DATE))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("env",         F.lit(ENV))
    )

    valid = df.count()
    logger.info(f"Validation: {total} rows in → {valid} valid ({total - valid} dropped)")
    return df


def pii_mask(df):
    """Hash member_id for PII compliance (HIPAA Safe Harbor)."""
    return df.withColumn("member_id", F.sha2(F.col("member_id"), 256))


def write_staging(df, path: str):
    (
        df.write
        .mode("overwrite")
        .partitionBy("claim_type")
        .parquet(path)
    )
    logger.info(f"Written to {path}")


def main():
    raw     = read_raw(RAW_PATH)
    cleaned = validate_and_clean(raw)
    masked  = pii_mask(cleaned)
    write_staging(masked, STAGING_PATH)


main()
job.commit()
