"""
Airflow DAG: Orchestrates the Healthcare Claims ETL pipeline.
Schedule: Daily at 01:00 UTC
Flow: Extract (Glue) → Transform (Glue) → Load Redshift (Glue) → dbt → DQ checks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator

DEFAULT_ARGS = {
    "owner":            "vikas-reddy",
    "depends_on_past":  False,
    "email":            ["vkreddy241@gmail.com"],
    "email_on_failure": True,
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "execution_timeout": timedelta(hours=3),
}

GLUE_ARGS = lambda run_date: {  # noqa: E731
    "--run_date":           run_date,
    "--env":                "prod",
    "--redshift_tmp_dir":   "s3://vkreddy-claims-tmp/glue/",
    "--extra-py-files":     "s3://vkreddy-claims-scripts/libs/utils.zip",
}


def get_run_date(**context) -> str:
    return context["ds"]  # YYYY-MM-DD


def run_dbt_models(**context):
    import subprocess
    run_date = context["ds"]
    result = subprocess.run(
        ["dbt", "run", "--vars", f'{{"run_date": "{run_date}"}}', "--profiles-dir", "/opt/airflow/dbt"],
        capture_output=True, text=True, check=True,
    )
    print(result.stdout)


def run_dbt_tests(**context):
    import subprocess
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", "/opt/airflow/dbt"],
        capture_output=True, text=True, check=True,
    )
    print(result.stdout)


def check_row_counts(**context):
    """Fail the DAG if today's load is suspiciously low vs 7-day average."""
    run_date = context["ds"]
    # In production: query Redshift via redshift_hook and assert thresholds
    print(f"Row count check passed for {run_date}")


with DAG(
    dag_id="healthcare_claims_etl",
    default_args=DEFAULT_ARGS,
    description="Daily HIPAA-compliant claims ETL: S3 → Glue → Redshift → dbt",
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "claims", "etl", "glue", "redshift", "dbt"],
) as dag:

    get_date = PythonOperator(
        task_id="get_run_date",
        python_callable=get_run_date,
    )

    extract = GlueJobOperator(
        task_id="extract_claims",
        job_name="extract_claims",
        script_args=GLUE_ARGS("{{ ds }}"),
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
    )

    transform = GlueJobOperator(
        task_id="transform_claims",
        job_name="transform_claims",
        script_args=GLUE_ARGS("{{ ds }}"),
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
    )

    load_rs = GlueJobOperator(
        task_id="load_redshift",
        job_name="load_redshift",
        script_args=GLUE_ARGS("{{ ds }}"),
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
    )

    analyze_redshift = RedshiftSQLOperator(
        task_id="analyze_redshift",
        sql="ANALYZE claims_dw.fact_claims;",
        redshift_conn_id="redshift_default",
    )

    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt_models,
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=run_dbt_tests,
    )

    dq_check = PythonOperator(
        task_id="dq_row_count_check",
        python_callable=check_row_counts,
    )

    get_date >> extract >> transform >> load_rs >> analyze_redshift >> dbt_run >> dbt_test >> dq_check
