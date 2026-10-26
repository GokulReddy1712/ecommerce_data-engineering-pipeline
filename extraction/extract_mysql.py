
import os
import csv
import io
import logging
from pathlib import Path

import mysql.connector
from google.cloud import storage
from dotenv import load_dotenv


# --------------------------------------------------
# 1. LOAD CONFIGURATION
# --------------------------------------------------

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))

INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")


# --------------------------------------------------
# 2. TABLES TO EXTRACT
# --------------------------------------------------

TABLES = {
    "customers": "raw/customers/customers.csv",
    "products": "raw/products/products.csv",
    "orders": "raw/orders/orders.csv",
    "order_items": "raw/order_items/order_items.csv"
}


# --------------------------------------------------
# 3. VALIDATE CONFIGURATION
# --------------------------------------------------

def validate_config():

    required = {
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD,
        "DB_NAME": DB_NAME,
        "GCS_BUCKET_NAME": GCS_BUCKET_NAME
    }

    missing = [
        key for key, value in required.items()
        if not value
    ]

    if missing:
        raise ValueError(
            f"Missing environment variables: {', '.join(missing)}"
        )


# --------------------------------------------------
# 4. CONNECT TO CLOUD SQL
# --------------------------------------------------

def create_connection():

    logging.info("Connecting to Cloud SQL MySQL...")

    if INSTANCE_CONNECTION_NAME:

        socket_path = f"/cloudsql/{INSTANCE_CONNECTION_NAME}"

        connection = mysql.connector.connect(
            unix_socket=socket_path,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            connection_timeout=30
        )

    else:

        if not DB_HOST:
            raise ValueError(
                "Set INSTANCE_CONNECTION_NAME or DB_HOST."
            )

        connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            connection_timeout=30
        )

    logging.info("Connected to Cloud SQL successfully.")

    return connection


# --------------------------------------------------
# 5. EXTRACT DATA FROM MYSQL
# --------------------------------------------------

def extract_table(connection, table_name):

    logging.info("Extracting table: %s", table_name)

    cursor = connection.cursor(dictionary=True)

    try:

        # Table names come only from our fixed TABLES dictionary.
        cursor.execute(f"SELECT * FROM `{table_name}`")

        records = cursor.fetchall()

        logging.info(
            "Extracted %s records from %s",
            len(records),
            table_name
        )

        return records

    finally:
        cursor.close()


# --------------------------------------------------
# 6. CONVERT DATA TO CSV IN MEMORY
# --------------------------------------------------

def convert_to_csv(records):

    if not records:
        raise ValueError("No records found. CSV creation cancelled.")

    csv_buffer = io.StringIO()

    writer = csv.DictWriter(
        csv_buffer,
        fieldnames=records[0].keys()
    )

    writer.writeheader()
    writer.writerows(records)

    return csv_buffer.getvalue()


# --------------------------------------------------
# 7. UPLOAD CSV DIRECTLY TO GCS
# --------------------------------------------------

def upload_to_gcs(storage_client, records, table_name, destination):

    csv_content = convert_to_csv(records)

    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    blob = bucket.blob(destination)

    blob.upload_from_string(
        csv_content,
        content_type="text/csv"
    )

    logging.info(
        "Uploaded successfully: gs://%s/%s",
        GCS_BUCKET_NAME,
        destination
    )


# --------------------------------------------------
# 8. MAIN EXTRACTION PIPELINE
# --------------------------------------------------

def main():

    connection = None

    try:

        validate_config()

        # Create one database connection.
        connection = create_connection()

        # Create one GCS client.
        storage_client = storage.Client()

        total_records = 0

        # Extract and upload every table.
        for table_name, destination in TABLES.items():

            records = extract_table(
                connection,
                table_name
            )

            upload_to_gcs(
                storage_client,
                records,
                table_name,
                destination
            )

            total_records += len(records)

        logging.info("----------------------------------")
        logging.info("ALL TABLES EXTRACTED SUCCESSFULLY")
        logging.info("Total records extracted: %s", total_records)
        logging.info("----------------------------------")

    except Exception:

        logging.exception(
            "MYSQL TO GCS EXTRACTION PIPELINE FAILED."
        )

        raise

    finally:

        if connection and connection.is_connected():

            connection.close()

            logging.info("Database connection closed.")


if __name__ == "__main__":
    main()
