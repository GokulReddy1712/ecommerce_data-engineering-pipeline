
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

PROJECT_ID = "project-3f0332f0-b63d-446c-91c"
BUCKET = "ecommerce-raw-data-3f0332f0"
DATASET = "ecommerce_analytics"

tables = ["customers", "products", "orders", "order_items"]

with DAG(
    dag_id="ecommerce_gcs_to_bigquery",
    description="Load curated e-commerce CSV files from GCS into BigQuery",
    start_date=datetime(2026, 10, 26),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "bigquery"],
) as dag:

    for table in tables:
        GCSToBigQueryOperator(
            task_id=f"load_{table}",
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
