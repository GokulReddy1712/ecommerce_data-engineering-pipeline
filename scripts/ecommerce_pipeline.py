
from datetime import datetime

from airflow import DAG

from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
)

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)


PROJECT_ID = "project-3f0332f0-b63d-446c-91c"
BUCKET = "ecommerce-raw-data-3f0332f0"
DATASET = "ecommerce_analytics"

REGION = "asia-south1"
CLUSTER_NAME = "ecommerce-spark-cluster-v2"


with DAG(
    dag_id="ecommerce_pipeline",
    description="End-to-end e-commerce data engineering pipeline",
    start_date=datetime(2026, 10, 26),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "data-engineering"],
) as dag:

    # STEP 1: Extract data from MySQL into GCS

    extract_mysql_to_gcs = CloudRunExecuteJobOperator(
        task_id="extract_mysql_to_gcs",
        project_id=PROJECT_ID,
        region="asia-south1",
        job_name="ecommerce-mysql-extractor",
    )


    # STEP 2: Submit PySpark transformation job to Dataproc

    transform_data = DataprocSubmitJobOperator(
        task_id="transform_data",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {
                "cluster_name": CLUSTER_NAME,
            },
            "pyspark_job": {
                "main_python_file_uri": (
                    f"gs://{BUCKET}/temp/transform_ecommerce.py"
                ),
            },
        },
    )


    # STEP 3: Load curated data into BigQuery

    tables = [
        "customers",
        "products",
        "orders",
        "order_items",
    ]

    load_tasks = []

    for table in tables:

        load_to_bigquery = GCSToBigQueryOperator(
            task_id=f"load_{table}_to_bigquery",
            bucket=BUCKET,
            source_objects=[f"curated/{table}/*.csv"],
            destination_project_dataset_table=(
                f"{PROJECT_ID}.{DATASET}.{table}"
            ),
            source_format="CSV",
            skip_leading_rows=1,
            autodetect=True,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
        )

        load_tasks.append(load_to_bigquery)


    # Define execution order

    extract_mysql_to_gcs >> transform_data >> load_tasks
