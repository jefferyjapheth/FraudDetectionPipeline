import logging
import pandas as pd
from io import StringIO
from src.config.settings import SCHEMA_REFERENCE

def file_type(file_key: str):
    """
    Determine the type of the file based on its S3 key prefix.
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


def validate_csv_schema(s3_hook, bucket, key, file_type, batch_id=None):
    """
    Validate the CSV file against the schema reference.

    Args:
        s3_hook: S3Hook instance
        bucket: str, S3 bucket name
        key: str, S3 object key
        file_type: str, type of file to validate
        batch_id: str, optional batch ID for idempotency tracking

    Returns:a
        dict: {
            "key": <file_key>,
            "file_type": <type>,
            "batch_id": <batch_id>,
            "valid": True/False
        }
    """
    result = {
        "key": key,
        "file_type": file_type,
        "batch_id": batch_id,
        "valid": False
    }

    if not key.lower().endswith(".csv"):
        logging.warning(f"Skipping non-CSV file: {key}")
        return result

    schema = SCHEMA_REFERENCE.get(file_type)
    if not schema:
        logging.warning(f"No schema found for {file_type}")
        return result

    try:
        obj = s3_hook.get_key(key, bucket_name=bucket)
        csv_data = obj.get()["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(csv_data))
        missing_cols = [c for c in schema.get("required_columns", []) if c not in df.columns]
        if missing_cols:
            logging.error(f"{key} missing columns: {missing_cols}")
            return result

        logging.info(f"✅ Schema validated for {file_type}: {key}")
        result["valid"] = True
        return result
    except Exception as e:
        logging.error(f"Failed to validate {key}: {e}")
        return result
