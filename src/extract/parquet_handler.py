"""
Parquet Handler for Large DataFrame XCom Transfer
Stores DataFrames as Parquet files in MinIO to avoid XCom size limits
"""
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from typing import Dict, List
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from src.config.settings import MASTER_DATA_BUCKET


def write_dataframe_to_parquet(
    df: pd.DataFrame,
    file_type: str,
    batch_id: str,
    hook: S3Hook = None
) -> str:
    """
    Write DataFrame to Parquet in MinIO and return the S3 key.
    
    Args:
        df: DataFrame to write
        file_type: Type identifier (customers, terminals, etc.)
        batch_id: Batch ID for organization
        hook: Optional S3Hook (creates new one if not provided)
        
    Returns:
        str: S3 key where Parquet file is stored
    """
    if hook is None:
        hook = S3Hook(aws_conn_id="minio_default")
    
    # Generate unique S3 key
    s3_key = f"temp/parquet/{batch_id}/{file_type}.parquet"
    
    try:
        # Convert DataFrame to Parquet bytes
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
        parquet_buffer.seek(0)
        
        # Upload to MinIO
        hook.load_bytes(
            parquet_buffer.getvalue(),
            key=s3_key,
            bucket_name=MASTER_DATA_BUCKET,
            replace=True
        )
        
        logging.info(f"✅ Wrote {len(df)} records to s3://{MASTER_DATA_BUCKET}/{s3_key}")
        return s3_key
        
    except Exception as e:
        logging.error(f"❌ Failed to write Parquet for {file_type}: {e}")
        raise


def read_dataframe_from_parquet(
    s3_key: str,
    hook: S3Hook = None
) -> pd.DataFrame:
    """
    Read DataFrame from Parquet file in MinIO.
    
    Args:
        s3_key: S3 key of the Parquet file
        hook: Optional S3Hook (creates new one if not provided)
        
    Returns:
        pd.DataFrame: Loaded DataFrame
    """
    if hook is None:
        hook = S3Hook(aws_conn_id="minio_default")
    
    try:
        # Download Parquet bytes from MinIO
        obj = hook.get_key(s3_key, bucket_name=MASTER_DATA_BUCKET)
        parquet_bytes = obj.get()["Body"].read()
        
        # Read Parquet to DataFrame
        parquet_buffer = BytesIO(parquet_bytes)
        df = pd.read_parquet(parquet_buffer, engine='pyarrow')
        
        logging.info(f"✅ Read {len(df)} records from s3://{MASTER_DATA_BUCKET}/{s3_key}")
        return df
        
    except Exception as e:
        logging.error(f"❌ Failed to read Parquet from {s3_key}: {e}")
        raise


def write_multiple_dataframes(
    dfs_dict: Dict[str, pd.DataFrame],
    batch_id: str,
    hook: S3Hook = None
) -> Dict[str, str]:
    """
    Write multiple DataFrames to Parquet files.
    
    Args:
        dfs_dict: Dictionary of DataFrames {type: DataFrame}
        batch_id: Batch ID for organization
        hook: Optional S3Hook
        
    Returns:
        Dict[str, str]: Dictionary of S3 keys {type: s3_key}
    """
    if hook is None:
        hook = S3Hook(aws_conn_id="minio_default")
    
    s3_keys = {}
    
    for file_type, df in dfs_dict.items():
        if df.empty:
            logging.warning(f"Skipping empty DataFrame for type: {file_type}")
            continue
            
        s3_key = write_dataframe_to_parquet(df, file_type, batch_id, hook)
        s3_keys[file_type] = s3_key
    
    logging.info(f"✅ Wrote {len(s3_keys)} DataFrames to Parquet")
    return s3_keys


def read_multiple_dataframes(
    s3_keys_dict: Dict[str, str],
    hook: S3Hook = None
) -> Dict[str, pd.DataFrame]:
    """
    Read multiple DataFrames from Parquet files.
    
    Args:
        s3_keys_dict: Dictionary of S3 keys {type: s3_key}
        hook: Optional S3Hook
        
    Returns:
        Dict[str, pd.DataFrame]: Dictionary of DataFrames {type: DataFrame}
    """
    if hook is None:
        hook = S3Hook(aws_conn_id="minio_default")
    
    dfs_dict = {}
    
    for file_type, s3_key in s3_keys_dict.items():
        df = read_dataframe_from_parquet(s3_key, hook)
        dfs_dict[file_type] = df
    
    logging.info(f"✅ Read {len(dfs_dict)} DataFrames from Parquet")
    return dfs_dict


def cleanup_temp_parquet_files(
    s3_keys: List[str],
    hook: S3Hook = None
) -> int:
    """
    Delete temporary Parquet files after processing.
    
    Args:
        s3_keys: List of S3 keys to delete
        hook: Optional S3Hook
        
    Returns:
        int: Number of files deleted
    """
    if hook is None:
        hook = S3Hook(aws_conn_id="minio_default")
    
    deleted_count = 0
    
    for s3_key in s3_keys:
        try:
            if hook.check_for_key(s3_key, bucket_name=MASTER_DATA_BUCKET):
                hook.delete_objects(bucket=MASTER_DATA_BUCKET, keys=[s3_key])
                deleted_count += 1
                logging.info(f"🗑️ Deleted temp Parquet: {s3_key}")
        except Exception as e:
            logging.warning(f"Failed to delete {s3_key}: {e}")
    
    logging.info(f"✅ Cleaned up {deleted_count} temp Parquet files")
    return deleted_count