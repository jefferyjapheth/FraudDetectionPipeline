"""
Parquet Handler for Large DataFrame XCom Transfer
-------------------------------------------------
This module provides utilities for efficiently storing and retrieving large
Pandas DataFrames as Parquet files in MinIO (S3-compatible storage). 

This approach is used to bypass Airflow’s XCom size limitations while maintaining
data integrity and high performance for intermediate data exchange between tasks.
"""

import logging
from io import BytesIO
from typing import Dict, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from src.config.settings import MASTER_DATA_BUCKET


# -----------------------------------------------------------------------------
# Core Write/Read Operations
# -----------------------------------------------------------------------------
def write_dataframe_to_parquet(
    df: pd.DataFrame,
    file_type: str,
    batch_id: str,
    hook: S3Hook = None
) -> str:
    """
    Write a Pandas DataFrame to a Parquet file and store it in MinIO.

    Args:
        df: DataFrame to serialize.
        file_type: Logical type identifier (e.g., 'customers', 'transactions').
        batch_id: Batch identifier for logical grouping of files.
        hook: Optional Airflow S3Hook instance. Created internally if not provided.

    Returns:
        str: The S3 object key where the Parquet file is stored.

    Raises:
        Exception: If Parquet serialization or upload fails.
    """
    if hook is None:
        hook = S3Hook(aws_conn_id="minio_default")

    s3_key = f"temp/parquet/{batch_id}/{file_type}.parquet"

    try:
        # Convert DataFrame to Parquet bytes in memory
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow", index=False)
        parquet_buffer.seek(0)

        # Upload serialized bytes to MinIO
        hook.load_bytes(
            parquet_buffer.getvalue(),
            key=s3_key,
            bucket_name=MASTER_DATA_BUCKET,
            replace=True,
        )

        logging.info(
            f"Wrote {len(df)} records to s3://{MASTER_DATA_BUCKET}/{s3_key}"
        )
        return s3_key

    except Exception as e:
        logging.error(f"Failed to write Parquet for {file_type}: {e}", exc_info=True)
        raise


def read_dataframe_from_parquet(
    s3_key: str,
    hook: S3Hook = None
) -> pd.DataFrame:
    """
    Read a Parquet file from MinIO into a Pandas DataFrame.

    Args:
        s3_key: S3 object key of the Parquet file to read.
        hook: Optional Airflow S3Hook instance. Created internally if not provided.

    Returns:
        pd.DataFrame: The deserialized DataFrame.

    Raises:
        Exception: If the file cannot be downloaded or deserialized.
    """
    if hook is None:
        hook = S3Hook(aws_conn_id="minio_default")

    try:
        # Fetch the Parquet object from MinIO
        obj = hook.get_key(s3_key, bucket_name=MASTER_DATA_BUCKET)
        parquet_bytes = obj.get()["Body"].read()

        # Deserialize Parquet bytes into a DataFrame
        df = pd.read_parquet(BytesIO(parquet_bytes), engine="pyarrow")

        logging.info(
            f"Read {len(df)} records from s3://{MASTER_DATA_BUCKET}/{s3_key}"
        )
        return df

    except Exception as e:
        logging.error(f"Failed to read Parquet from {s3_key}: {e}", exc_info=True)
        raise


# -----------------------------------------------------------------------------
# Batch Operations
# -----------------------------------------------------------------------------
def write_multiple_dataframes(
    dfs_dict: Dict[str, pd.DataFrame],
    batch_id: str,
    hook: S3Hook = None
) -> Dict[str, str]:
    """
    Write multiple DataFrames to Parquet files in MinIO.

    Args:
        dfs_dict: Mapping of {file_type: DataFrame}.
        batch_id: Batch identifier for grouping files in storage.
        hook: Optional Airflow S3Hook instance.

    Returns:
        Dict[str, str]: Mapping of {file_type: s3_key} for written files.
    """
    if hook is None:
        hook = S3Hook(aws_conn_id="minio_default")

    s3_keys = {}

    for file_type, df in dfs_dict.items():
        if df.empty:
            logging.warning(f"Skipping empty DataFrame for type '{file_type}'.")
            continue

        try:
            s3_key = write_dataframe_to_parquet(df, file_type, batch_id, hook)
            s3_keys[file_type] = s3_key
        except Exception:
            logging.error(f"Error writing {file_type} to Parquet.", exc_info=True)

    logging.info(f"Wrote {len(s3_keys)} DataFrames to Parquet for batch {batch_id}.")
    return s3_keys


def read_multiple_dataframes(
    s3_keys_dict: Dict[str, str],
    hook: S3Hook = None
) -> Dict[str, pd.DataFrame]:
    """
    Read multiple Parquet files from MinIO into DataFrames.

    Args:
        s3_keys_dict: Mapping of {file_type: s3_key}.
        hook: Optional Airflow S3Hook instance.

    Returns:
        Dict[str, pd.DataFrame]: Mapping of {file_type: DataFrame}.
    """
    if hook is None:
        hook = S3Hook(aws_conn_id="minio_default")

    dfs_dict = {}

    for file_type, s3_key in s3_keys_dict.items():
        try:
            df = read_dataframe_from_parquet(s3_key, hook)
            dfs_dict[file_type] = df
        except Exception:
            logging.error(f"Failed to read DataFrame for '{file_type}'.", exc_info=True)

    logging.info(f"Read {len(dfs_dict)} DataFrames from Parquet files.")
    return dfs_dict


# -----------------------------------------------------------------------------
# Cleanup Operations
# -----------------------------------------------------------------------------
def cleanup_temp_parquet_files(
    s3_keys: List[str],
    hook: S3Hook = None
) -> int:
    """
    Remove temporary Parquet files from MinIO once processing is complete.

    Args:
        s3_keys: List of S3 object keys to delete.
        hook: Optional Airflow S3Hook instance.

    Returns:
        int: Number of successfully deleted files.
    """
    if hook is None:
        hook = S3Hook(aws_conn_id="minio_default")

    deleted_count = 0

    for s3_key in s3_keys:
        try:
            if hook.check_for_key(s3_key, bucket_name=MASTER_DATA_BUCKET):
                hook.delete_objects(bucket=MASTER_DATA_BUCKET, keys=[s3_key])
                deleted_count += 1
                logging.info(f"Deleted temporary Parquet file: {s3_key}")
        except Exception as e:
            logging.warning(f"Failed to delete {s3_key}: {e}")

    logging.info(f"Cleaned up {deleted_count} temporary Parquet files.")
    return deleted_count
