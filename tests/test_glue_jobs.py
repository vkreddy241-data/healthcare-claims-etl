"""
Unit tests for Glue ETL logic using PySpark local mode.
Run: pytest tests/ -v
"""

import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, DateType,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("TestClaimsETL")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


CLAIMS_SCHEMA = StructType([
    StructField("claim_id",       StringType(), True),
    StructField("member_id",      StringType(), True),
    StructField("provider_npi",   StringType(), True),
    StructField("billed_amount",  DoubleType(), True),
    StructField("paid_amount",    DoubleType(), True),
    StructField("claim_status",   StringType(), True),
    StructField("claim_type",     StringType(), True),
    StructField("service_date",   DateType(),   True),
    StructField("discharge_date", DateType(),   True),
])

SAMPLE_ROWS = [
    ("C001", "M001", "NPI001", 1000.0, 800.0,  "APPROVED", "medical",  date(2024, 1, 10), date(2024, 1, 12)),
    ("C002", "M002", "NPI002", 500.0,  0.0,    "DENIED",   "pharmacy", date(2024, 1, 11), None),
    ("C003", None,   "NPI003", 200.0,  150.0,  "APPROVED", "dental",   date(2024, 1, 12), None),
    ("C004", "M004", "NPI004", -50.0,  0.0,    "VOID",     "medical",  date(2024, 1, 13), None),
    ("C005", "M005", "NPI005", 25000.0, 22000.0, "APPROVED", "medical", date(2024, 1, 14), date(2024, 1, 20)),
]


@pytest.fixture
def raw_df(spark):
    return spark.createDataFrame(SAMPLE_ROWS, CLAIMS_SCHEMA)


class TestValidation:
    def test_drops_null_member_id(self, spark, raw_df):
        cleaned = raw_df.dropna(subset=["claim_id", "member_id"])
        assert cleaned.count() == 4   # C003 dropped

    def test_drops_negative_amounts(self, spark, raw_df):
        cleaned = raw_df.filter(F.col("billed_amount") >= 0)
        assert cleaned.count() == 4   # C004 dropped

    def test_all_claim_ids_present(self, spark, raw_df):
        ids = {r.claim_id for r in raw_df.collect()}
        assert "C001" in ids


class TestBusinessRules:
    def test_denied_flag(self, spark, raw_df):
        df = raw_df.withColumn("denied_flag", F.col("claim_status") == "DENIED")
        denied = df.filter(F.col("denied_flag")).count()
        assert denied == 1   # only C002

    def test_high_cost_flag(self, spark, raw_df):
        df = raw_df.withColumn("high_cost_flag", F.col("paid_amount") > 10000)
        high = df.filter(F.col("high_cost_flag")).count()
        assert high == 1   # only C005

    def test_los_calculation(self, spark, raw_df):
        df = raw_df.withColumn(
            "los", F.datediff(F.col("discharge_date"), F.col("service_date"))
        )
        c001_los = df.filter(F.col("claim_id") == "C001").select("los").first()["los"]
        assert c001_los == 2


class TestPIIMasking:
    def test_member_id_hashed(self, spark, raw_df):
        masked = raw_df.withColumn("member_id", F.sha2(F.col("member_id"), 256))
        original_ids = {"M001", "M002", "M004", "M005"}
        masked_ids = {r.member_id for r in masked.dropna(subset=["member_id"]).collect()}
        assert original_ids.isdisjoint(masked_ids)

    def test_hash_length(self, spark, raw_df):
        masked = raw_df.withColumn("member_id", F.sha2(F.col("member_id"), 256))
        lengths = {len(r.member_id) for r in masked.dropna(subset=["member_id"]).collect()}
        assert lengths == {64}   # SHA-256 hex = 64 chars
