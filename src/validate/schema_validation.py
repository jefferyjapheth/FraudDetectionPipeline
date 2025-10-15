"""
CSV Schema Validation Utilities
-------------------------------
This module provides functionality to:
- Determine file type from S3 key naming conventions.
- Validate CSV files stored in S3 (MinIO) against predefined schema references.

Used primarily during ingestion to enforce structural integrity
before data transformation or loading into Iceberg tables.
"""

import logging
import pandas as pd
from io import StringIO
from src.config.settings import SCHEMA_REFERENCE


# -----------------------------------------------------------------------------
# File Type Identification
# -----------------------------------------------------------------------------
def file_type(file_key: str) -> str:
    """
    Determine the logical data type of a file based on its S3 key prefix.

    Args:
        file_key (str): Full S3 object key (path within the bucket).

    Returns:
        str: One of {"customers", "terminals", "transactions", "travel_profiles"}.
             Returns None if the prefix does not match any known type.

    Example:
        >>> file_type("customers/data_2025_10_01.csv")
        'customers'
    """
    if file_key.startswith("customers/"):
        return "customers"
    if file_key.startswith("terminals/"):
        return "terminals"
    if file_key.startswith("transactions/"):
        return "transactions"
    if file_key.startswith("travel_profiles/"):
        return "travel_profiles"
    return None


# -----------------------------------------------------------------------------
# Schema Validation Logic
# -----------------------------------------------------------------------------
def validate_csv_schema(s3_hook, bucket: str, key: str, file_type: str, batch_id: str = None) -> dict:
    """
    Validate a CSV file stored in S3 (MinIO) against its expected schema definition.

    The validation process checks:
        - File extension (.csv)
        - Presence of required columns defined in SCHEMA_REFERENCE

    Args:
        s3_hook: Airflow S3Hook instance configured for MinIO access.
        bucket (str): S3 bucket name containing the file.
        key (str): Object key (path to the file) within the bucket.
        file_type (str): Logical type of the file, as determined by `file_type()`.
        batch_id (str, optional): Optional batch identifier for traceability.

    Returns:
        dict: Validation result summary in the format:
            {
                "key": "<file_key>",
                "file_type": "<type>",
                "batch_id": "<batch_id>",
                "valid": True | False
            }

    Behavior:
        - Logs and skips non-CSV files.
        - Logs missing columns if schema requirements are not met.
        - Returns a structured result for downstream tracking.
    """
    result = {
        "key": key,
        "file_type": file_type,
        "batch_id": batch_id,
        "valid": False
    }

    # Validate file type and extension
    if not key.lower().endswith(".csv"):
        logging.warning(f"Skipping non-CSV file: {key}")
        return result

    # Retrieve schema for file type
    schema = SCHEMA_REFERENCE.get(file_type)
    if not schema:
        logging.warning(f"No schema reference found for file type '{file_type}'")
        return result

    try:
        # Read CSV from S3 into DataFrame
        obj = s3_hook.get_key(key, bucket_name=bucket)
        csv_data = obj.get()["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(csv_data))

        # Verify required columns
        required_cols = schema.get("required_columns", [])
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            logging.error(f"Schema validation failed for {key}. Missing columns: {missing_cols}")
            return result

        logging.info(f"Schema validated successfully for {file_type}: {key}")
        result["valid"] = True
        return result

    except Exception as e:
        logging.error(f"Error validating schema for {key}: {e}")
        return result
