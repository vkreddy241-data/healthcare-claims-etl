"""
AWS Glue Job: Load conformed claims from S3 Delta Lake into Redshift.
Uses COPY command via S3 staging for high-throughput bulk loads.

Input:   s3://vkreddy-claims-lake/conformed/claims/
Output:  Redshift — claims_dw schema
"""

import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "run_date", "redshift_tmp_dir"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RUN_DATE      = args["run_date"]
REDSHIFT_TMP  = args["redshift_tmp_dir"]
CONFORMED     = "s3://vkreddy-claims-lake/conformed/claims/"

REDSHIFT_OPTIONS = {
    "url":             "jdbc:redshift://vkreddy-claims.cluster.us-east-1.redshift.amazonaws.com:5439/claims_dw",
    "dbtable":         "claims_dw.fact_claims",
    "user":            "glue_etl_user",
    "password":        "{{ssm:/claims/redshift/password}}",   # resolved at runtime via Secrets Manager
    "redshiftTmpDir":  REDSHIFT_TMP,
    "aws_iam_role":    "arn:aws:iam::123456789012:role/RedshiftCopyRole",
}

LOAD_COLS = [
    "claim_id", "member_id", "provider_npi", "service_date", "discharge_date",
    "diagnosis_code", "procedure_code", "billed_amount", "allowed_amount",
    "paid_amount", "claim_status", "claim_type", "plan_id",
    "denied_flag", "high_cost_flag", "out_of_network_flag",
    "length_of_stay_days", "cost_ratio", "service_year", "service_month",
    "provider_name", "specialty", "network_flag", "state",
    "ingested_at", "run_date",
]


def main():
    df = (
        spark.read.format("delta")
        .load(CONFORMED)
        .filter(F.col("run_date") == RUN_DATE)
        .select(LOAD_COLS)
    )

    row_count = df.count()
    logger.info(f"Loading {row_count} rows into Redshift for run_date={RUN_DATE}")

    glueContext.write_dynamic_frame.from_options(
        frame=glueContext.create_dynamic_frame.from_dataframe(df, glueContext),
        connection_type="redshift",
        connection_options=REDSHIFT_OPTIONS,
    )

    logger.info("Redshift load complete.")


main()
job.commit()
