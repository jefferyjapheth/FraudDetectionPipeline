import os
import json
import logging

RAW_BUCKET = "raw-data"
STAGING_BUCKET = "staging-data"
ARCHIVE_BUCKET = "archive-data"

CHUNK_SIZE = 4
MAX_TOTAL_TASKS = 500

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
SCHEMA_PATH = os.path.join(AIRFLOW_HOME, "include", "schema_reference.json")

try:
    with open(SCHEMA_PATH, "r") as f:
        SCHEMA_REFERENCE = json.load(f)
    logging.info(f"✅ Loaded schema from {SCHEMA_PATH}")
except (FileNotFoundError, json.JSONDecodeError) as e:
    logging.error(f"Failed to load schema reference: {e}")
    SCHEMA_REFERENCE = {}
