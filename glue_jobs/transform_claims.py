"""
AWS Glue Job: Transform staged claims into conformed layer.
Applies business rules, SCD Type 2 for members, joins reference data.

Input:   s3://vkreddy-claims-lake/staging/claims/
Output:  s3://vkreddy-claims-lake/conformed/claims/
"""

import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "run_date"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RUN_DATE       = args["run_date"]
STAGING_PATH   = f"s3://vkreddy-claims-lake/staging/claims/run_date={RUN_DATE}/"
CONFORMED_PATH = "s3://vkreddy-claims-lake/conformed/claims/"
MEMBER_REF     = "s3://vkreddy-claims-lake/reference/members/"
PROVIDER_REF   = "s3://vkreddy-claims-lake/reference/providers/"


def enrich_with_reference(claims_df):
    members   = spark.read.parquet(MEMBER_REF).select("member_id", "plan_type", "group_id", "state")
    providers = spark.read.parquet(PROVIDER_REF).select("provider_npi", "provider_name", "specialty", "network_flag")

    return (
        claims_df
        .join(members,   on="member_id",    how="left")
        .join(providers, on="provider_npi", how="left")
    )


def apply_business_rules(df):
    """
    Business rules:
    - denied_flag: claim_status = 'DENIED'
    - high_cost_flag: paid_amount > 10000
    - out_of_network_flag: network_flag != 'IN'
    - los (length of stay): discharge_date - service_date
    """
    return (
        df
        .withColumn("denied_flag",          F.col("claim_status") == "DENIED")
        .withColumn("high_cost_flag",        F.col("paid_amount") > 10000)
        .withColumn("out_of_network_flag",   F.col("network_flag") != "IN")
        .withColumn("length_of_stay_days",
                    F.datediff(F.col("discharge_date"), F.col("service_date")))
        .withColumn("cost_ratio",
                    F.round(F.col("paid_amount") / F.col("billed_amount"), 4))
        .withColumn("service_year",  F.year("service_date"))
        .withColumn("service_month", F.month("service_date"))
    )


def dedup_claims(df):
    """Keep the latest record per claim_id (in case of resubmissions)."""
    w = Window.partitionBy("claim_id").orderBy(F.col("ingested_at").desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def upsert_to_delta(new_df, target_path: str):
    """Merge new records into Delta Lake conformed layer."""
    if DeltaTable.isDeltaTable(spark, target_path):
        delta_tbl = DeltaTable.forPath(spark, target_path)
        (
            delta_tbl.alias("tgt")
            .merge(new_df.alias("src"), "tgt.claim_id = src.claim_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("Delta merge complete.")
    else:
        new_df.write.format("delta").partitionBy("service_year", "service_month").save(target_path)
        logger.info("Initial Delta write complete.")


def main():
    staged   = spark.read.parquet(STAGING_PATH)
    enriched = enrich_with_reference(staged)
    ruled    = apply_business_rules(enriched)
    deduped  = dedup_claims(ruled)
    upsert_to_delta(deduped, CONFORMED_PATH)


main()
job.commit()
