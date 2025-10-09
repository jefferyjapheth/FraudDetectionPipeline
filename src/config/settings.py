import os
import json
import logging

RAW_BUCKET = "raw-data"
MASTER_DATA_BUCKET = "master-data"  # Changed from STAGING_BUCKET
ARCHIVE_BUCKET = "archive-data"

CHUNK_SIZE = 4
MAX_TOTAL_TASKS = 500

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
SCHEMA_PATH = os.path.join(AIRFLOW_HOME, "include", "schema_reference.json")

# Iceberg catalog configuration
ICEBERG_CATALOG_CONFIG = {
    "type": "sql",
    "uri": "postgresql://postgres:postgres@postgres:5432/postgres",
    "warehouse": f"s3://{MASTER_DATA_BUCKET}/warehouse",
    "s3.endpoint": "http://host.docker.internal:9000",
    "s3.access-key-id": "admin",
    "s3.secret-access-key": "password",
    "s3.path-style-access": "true",
    "s3.region": "us-east-1"
}



ICEBERG_NAMESPACE = "masterData"

try:
    with open(SCHEMA_PATH, "r") as f:
        SCHEMA_REFERENCE = json.load(f)
    logging.info(f" Loaded schema from {SCHEMA_PATH}")
except (FileNotFoundError, json.JSONDecodeError) as e:
    logging.error(f"Failed to load schema reference: {e}")
    SCHEMA_REFERENCE = {}