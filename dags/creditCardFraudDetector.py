from airflow.sdk import dag, task, Asset, task_group
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging
import json
import pandas as pd
import pyarrow as pa
from io import StringIO
from typing import List, Dict

from src.config.settings import RAW_BUCKET, ARCHIVE_BUCKET
from src.validate.schema_validation import file_type, validate_csv_schema
from src.validate.chunking import create_chunks_safe
from src.extract.s3_operations import move_file, archive_processed_files
from src.extract.parquet_handler import (
    write_multiple_dataframes,
    read_dataframe_from_parquet,
    cleanup_temp_parquet_files
)
from src.transform.transformation import prepare_for_iceberg
from src.transform.iceberg_manager import initialize_iceberg_tables, upsert_to_iceberg

# -----------------------------
# Assets
# -----------------------------
RAW_CSV_ASSET = Asset("s3://raw-data/creditcard_files")
ICEBERG_ASSET = Asset("iceberg://master_data/tables")
PROCESSED_ASSET = Asset("s3://archive-data/processed_creditcard_files")

# -----------------------------
# DAG
# -----------------------------
@dag(
    start_date=datetime(2025, 9, 18),
    schedule=[RAW_CSV_ASSET],
    catchup=False,
    tags=["minio", "iceberg", "creditcardfraud", "etl", "parallel", "parquet"],
    max_active_runs=1,
)
def creditcardfraud_dag():
    """
    Credit Card Fraud Detection ETL Pipeline (Parquet XCom Architecture)
    
    Flow:
    1. Setup Iceberg tables
    2. Validate incoming CSV files from metadata
    3. Filter and chunk files by type (customers, terminals, transactions, travel_profiles)
    4. Transform chunks in parallel (no joins yet)
    5. Combine chunks by type
    6. Perform joins across types (transactions enriched with dimensions)
    7. Write master DataFrames to Parquet in MinIO (avoid XCom size limits)
    8. Upsert to Iceberg (reads from Parquet, prepares, converts to PyArrow)
    9. Archive processed files and cleanup temp Parquet files
    """

    # -----------------------------
    # Initialize Iceberg tables
    # -----------------------------
    @task
    def setup_iceberg():
        """Initialize Iceberg tables before processing"""
        initialize_iceberg_tables()
        return {"status": "initialized"}

    # -----------------------------
    # Validate files from metadata
    # -----------------------------
    @task
    def validate_files(setup_result, triggering_asset_events=None):
        """
        Validate incoming CSV files and separate valid from invalid.
        Returns dict with 'valid' and 'invalid' lists.
        """
        hook = S3Hook(aws_conn_id="minio_default")
        valid_files, invalid_files = [], []

        metadata_key = "metadata/metadata_latest.json"
        try:
            obj = hook.get_key(metadata_key, bucket_name=RAW_BUCKET)
            metadata = json.loads(obj.get()["Body"].read())
            batch_id = metadata.get("batch_id")
            file_list = [
                f["s3_path"].replace(f"s3://{RAW_BUCKET}/", "") 
                for f in metadata.get("files", [])
            ]
        except Exception as e:
            logging.warning(f"Failed to read metadata_latest.json: {e}")
            batch_id = None
            file_list = []

        if not file_list:
            logging.warning("No files found for validation")
            return {"valid": [], "invalid": [], "batch_id": batch_id}

        for file_key in file_list:
            ftype = file_type(file_key)
            if not ftype or not file_key.endswith(".csv"):
                move_file(hook, RAW_BUCKET, ARCHIVE_BUCKET, file_key, f"invalid/{file_key}")
                invalid_files.append({"file": file_key, "batch_id": batch_id})
                continue

            validation_result = validate_csv_schema(hook, RAW_BUCKET, file_key, ftype, batch_id)
            
            if validation_result.get("valid", False):
                valid_files.append({
                    "file": file_key,
                    "type": ftype,
                    "batch_id": batch_id
                })
            else:
                move_file(hook, RAW_BUCKET, ARCHIVE_BUCKET, file_key, f"invalid/{file_key}")
                invalid_files.append({"file": file_key, "batch_id": batch_id})

        logging.info(f"✅ Valid: {len(valid_files)} | ❌ Invalid: {len(invalid_files)}")
        return {"valid": valid_files, "invalid": invalid_files, "batch_id": batch_id}

    # -----------------------------
    # Split & filter by type
    # -----------------------------
    @task
    def split_by_type(valid_dict):
        """Extract valid files from validation result"""
        return valid_dict.get("valid", [])

    @task
    def filter_by_type(files, target_type):
        """Filter files by their type"""
        filtered = [f for f in files if f["type"] == target_type]
        logging.info(f"Filtered {len(filtered)} files for type: {target_type}")
        return filtered

    # -----------------------------
    # Chunking
    # -----------------------------
    @task
    def create_chunks(files, file_type_val):
        """
        Create chunks from files for parallel processing.
        Uses create_chunks_safe which returns List[List[Dict]].
        """
        if not files:
            logging.info(f"No files to chunk for type: {file_type_val}")
            return []
        
        chunks = create_chunks_safe(files)
        
        # Add metadata to each chunk for tracking
        formatted_chunks = []
        for idx, chunk in enumerate(chunks):
            formatted_chunks.append({
                "chunk_id": f"{file_type_val}_chunk_{idx}",
                "type": file_type_val,
                "files": chunk,
                "chunk_index": idx
            })
        
        logging.info(f"Created {len(formatted_chunks)} chunks from {len(files)} files for type: {file_type_val}")
        return formatted_chunks

    # -----------------------------
    # Parallel transformation of individual chunks
    # -----------------------------
    @task
    def transform_single_chunk(chunk_data):
        """
        Transform a single chunk in parallel.
        Reads files from S3, applies basic transformations, returns serialized data.
        NO JOINS happen here - just individual chunk transformation.
        
        Args:
            chunk_data: Dict with chunk_id, type, files (list of file dicts)
            
        Returns:
            Dict with transformed DataFrame data and metadata
        """
        if not chunk_data or not chunk_data.get("files"):
            logging.warning(f"Empty or invalid chunk received: {chunk_data}")
            return None
        
        hook = S3Hook(aws_conn_id="minio_default")
        file_type_val = chunk_data.get("type")
        files = chunk_data.get("files", [])
        chunk_id = chunk_data.get("chunk_id", "unknown")
        
        logging.info(f"🔄 Processing chunk {chunk_id} with {len(files)} files")
        
        try:
            all_data = []
            file_keys = []
            
            for f in files:
                file_key = f.get("file")
                batch_id = f.get("batch_id")
                
                try:
                    obj = hook.get_key(file_key, bucket_name=RAW_BUCKET)
                    csv_data = obj.get()["Body"].read().decode("utf-8")
                    df = pd.read_csv(StringIO(csv_data))
                    
                    # Add metadata columns
                    df["BATCH_ID"] = batch_id
                    df["created_at"] = pd.Timestamp.utcnow()
                    
                    all_data.append(df)
                    file_keys.append(file_key)
                    logging.info(f"Read {len(df)} rows from {file_key}")
                    
                except Exception as e:
                    logging.error(f"Failed to read {file_key} from S3: {e}")
                    continue
            
            if not all_data:
                logging.warning(f"No data read for chunk {chunk_id}")
                return None
            
            # Combine all DataFrames in this chunk
            df_combined = pd.concat(all_data, ignore_index=True)
            logging.info(f"Combined {len(df_combined)} records from {len(all_data)} files")
            
            # Apply type-specific basic transformations
            if file_type_val in ["customers", "terminals"]:
                if "effective_date" not in df_combined.columns:
                    df_combined["effective_date"] = pd.Timestamp.utcnow()
                if "version" not in df_combined.columns:
                    df_combined["version"] = 1
                    
            elif file_type_val == "transactions":
                if "TX_DATETIME" in df_combined.columns:
                    df_combined["TX_DATETIME"] = pd.to_datetime(df_combined["TX_DATETIME"], utc=True)
            
            logging.info(f"✅ Chunk {chunk_id} transformed: {len(df_combined)} records")
            
            # Convert datetime columns to strings for XCom serialization
            datetime_cols = df_combined.select_dtypes(
                include=['datetime64[ns, UTC]', 'datetime64[ns]', 'datetime64']
            ).columns
            for col in datetime_cols:
                df_combined[col] = df_combined[col].astype(str)
            
            # Serialize DataFrame for XCom
            return {
                "chunk_id": chunk_id,
                "type": file_type_val,
                "data": df_combined.to_dict('records'),
                "columns": list(df_combined.columns),
                "datetime_columns": list(datetime_cols),
                "record_count": len(df_combined),
                "files": file_keys
            }
                
        except Exception as e:
            logging.error(f"❌ Error transforming chunk {chunk_id}: {e}")
            raise

    # -----------------------------
    # Combine chunks by type (no joins yet)
    # -----------------------------
    @task
    def combine_chunks_by_type(transformed_chunks_list):
        """
        Combine all transformed chunks, grouped by type.
        NO JOINS happen here - just combining chunks of the same type.
        
        Args:
            transformed_chunks_list: List of lists of transformed chunks per type
            
        Returns:
            Dict of combined DataFrames by type {type: DataFrame}
        """
        # Flatten nested structure from multiple task groups
        all_chunks = []
        
        if hasattr(transformed_chunks_list, '__iter__'):
            for item in transformed_chunks_list:
                if item is None:
                    continue
                if hasattr(item, '__iter__') and not isinstance(item, dict):
                    for chunk in item:
                        if chunk is not None:
                            all_chunks.append(chunk)
                else:
                    all_chunks.append(item)
        
        if not all_chunks:
            logging.warning("No valid chunks to combine")
            return {}
        
        logging.info(f"📦 Combining {len(all_chunks)} total chunks across all types")
        
        # Group chunks by type
        chunks_by_type = {}
        for chunk in all_chunks:
            if not isinstance(chunk, dict):
                logging.warning(f"Skipping non-dict chunk: {type(chunk)}")
                continue
                
            chunk_type = chunk.get("type")
            if chunk_type not in chunks_by_type:
                chunks_by_type[chunk_type] = []
            chunks_by_type[chunk_type].append(chunk)
        
        # Combine chunks of each type into single DataFrames
        combined_dfs = {}
        for file_type_val, type_chunks in chunks_by_type.items():
            logging.info(f"Combining {len(type_chunks)} chunks for type: {file_type_val}")
            
            type_dfs = []
            datetime_cols = set()
            
            for chunk in type_chunks:
                df = pd.DataFrame(chunk["data"])
                
                if "datetime_columns" in chunk:
                    datetime_cols.update(chunk["datetime_columns"])
                
                type_dfs.append(df)
            
            if type_dfs:
                combined_df = pd.concat(type_dfs, ignore_index=True)
                
                # Reconvert datetime columns from strings
                for col in datetime_cols:
                    if col in combined_df.columns:
                        combined_df[col] = pd.to_datetime(combined_df[col], utc=True)
                
                combined_dfs[file_type_val] = combined_df
                logging.info(f"✅ Combined {file_type_val}: {len(combined_df)} records")
        
        return combined_dfs

    # -----------------------------
    # Perform joins across types
    # -----------------------------
    @task
    def join_master_data(combined_dfs):
        """
        Perform joins across all data types to create master DataFrames.
        Enriches transactions with customers, terminals, and travel_profiles.
        
        Args:
            combined_dfs: Dict of DataFrames by type {type: DataFrame}
            
        Returns:
            Dict of master DataFrames ready for Parquet storage
        """
        if not combined_dfs:
            logging.warning("No data to join")
            return {}
        
        logging.info(f"🔗 Performing joins across {len(combined_dfs)} data types")
        
        master_dfs = {}
        
        # Pass through dimension tables as-is
        for dim in ["customers", "terminals", "travel_profiles"]:
            if dim in combined_dfs:
                master_dfs[dim] = combined_dfs[dim]
                logging.info(f"✅ {dim}: {len(combined_dfs[dim])} records (no join needed)")
        
        # Handle transactions with joins
        if "transactions" in combined_dfs:
            df_tx = combined_dfs["transactions"].copy()
            logging.info(f"Starting with {len(df_tx)} transactions")
            
            # Join with customers (left join to keep all transactions)
            if "customers" in combined_dfs:
                df_customers = combined_dfs["customers"]
                join_cols = ["CUSTOMER_ID", "HOME_CITY", "HOME_REGION"]
                available_cols = [col for col in join_cols if col in df_customers.columns]
                
                if available_cols:
                    df_tx = df_tx.merge(
                        df_customers[available_cols],
                        on="CUSTOMER_ID",
                        how="left"
                    )
                    logging.info(f"✅ Joined with customers: {len(df_tx)} records")
            
            # Join with terminals (left join to keep all transactions)
            if "terminals" in combined_dfs:
                df_terminals = combined_dfs["terminals"]
                join_cols = ["TERMINAL_ID", "CITY", "REGION"]
                available_cols = [col for col in join_cols if col in df_terminals.columns]
                
                if available_cols:
                    df_tx = df_tx.merge(
                        df_terminals[available_cols],
                        on="TERMINAL_ID",
                        how="left",
                        suffixes=("", "_terminal")
                    )
                    logging.info(f"✅ Joined with terminals: {len(df_tx)} records")
            
            # Join with travel profiles (left join, optional enrichment)
            if "travel_profiles" in combined_dfs:
                df_travel = combined_dfs["travel_profiles"]
                join_cols = ["CUSTOMER_ID", "TRAVEL_REGIONS", "AVG_TRAVEL_DISTANCE_KM"]
                available_cols = [col for col in join_cols if col in df_travel.columns]
                
                if "CUSTOMER_ID" in available_cols:
                    df_tx = df_tx.merge(
                        df_travel[available_cols],
                        on="CUSTOMER_ID",
                        how="left"
                    )
                    logging.info(f"✅ Joined with travel profiles: {len(df_tx)} records")
            
            master_dfs["transactions"] = df_tx
            logging.info(f"✅ Final transactions DataFrame: {len(df_tx)} records")
        
        logging.info(f"🎉 Created master DataFrames for {len(master_dfs)} types")
        return master_dfs

    # -----------------------------
    # Write master DataFrames to Parquet (avoid XCom size limits)
    # -----------------------------
    @task
    def write_to_parquet(master_dfs, validated):
        """
        Write master DataFrames to Parquet files in MinIO.
        Returns S3 keys instead of large DataFrames to avoid XCom limits.
        
        Args:
            master_dfs: Dict of master DataFrames {type: DataFrame}
            validated: Validation result containing batch_id
            
        Returns:
            Dict of S3 keys {type: s3_key}
        """
        if not isinstance(master_dfs, dict):
            logging.error(f"Expected dict, got {type(master_dfs)}")
            return {}
        
        if not master_dfs:
            logging.warning("No master DataFrames to write")
            return {}
        
        # Extract batch_id from validated result
        batch_id = validated.get("batch_id", "unknown_batch")
        
        hook = S3Hook(aws_conn_id="minio_default")
        s3_keys = write_multiple_dataframes(master_dfs, batch_id, hook)
        
        logging.info(f"🎉 Wrote {len(s3_keys)} DataFrames to Parquet in MinIO")
        return s3_keys

    # -----------------------------
    # Upsert to Iceberg - Process each type separately
    # -----------------------------
    @task(outlets=[ICEBERG_ASSET])
    def upsert_single_type(s3_keys_dict, file_type_val):
        """
        Upsert a single data type to Iceberg.
        Reads DataFrame from Parquet, prepares it, converts to PyArrow, and upserts.
        
        Args:
            s3_keys_dict: Dict of S3 keys {type: s3_key}
            file_type_val: Type to upsert
            
        Returns:
            Dict with upsert result metadata
        """
        if not isinstance(s3_keys_dict, dict):
            logging.error(f"Expected dict, got {type(s3_keys_dict)}")
            return {"type": file_type_val, "records": 0, "status": "failed"}
        
        if file_type_val not in s3_keys_dict:
            logging.warning(f"Type {file_type_val} not in s3_keys_dict")
            return {"type": file_type_val, "records": 0, "status": "skipped"}
        
        s3_key = s3_keys_dict[file_type_val]
        
        try:
            # Read DataFrame from Parquet
            hook = S3Hook(aws_conn_id="minio_default")
            df = read_dataframe_from_parquet(s3_key, hook)
            
            if df.empty:
                logging.info(f"Skipping empty DataFrame for type: {file_type_val}")
                return {"type": file_type_val, "records": 0, "status": "empty"}
            
            logging.info(f"📋 Preparing {file_type_val} for Iceberg: {len(df)} records")
            
            # Prepare DataFrame (schema enforcement, transformations)
            # prepare_for_iceberg now returns DataFrame, not PyArrow Table
            prepared_df = prepare_for_iceberg(df)
            
            # Convert to PyArrow Table inline (happens here, not passed through XCom)
            arrow_table = pa.Table.from_pandas(prepared_df)
            
            logging.info(f"📤 Upserting {len(arrow_table)} records for type: {file_type_val}")
            
            # Upsert to Iceberg
            result = upsert_to_iceberg(file_type_val, arrow_table)
            logging.info(f"✅ Upserted {result.get('records', 0)} records for {file_type_val}")
            
            return result
            
        except Exception as e:
            logging.error(f"❌ Failed to upsert {file_type_val}: {e}")
            raise

    # -----------------------------
    # Archive and cleanup
    # -----------------------------
    @task(outlets=[PROCESSED_ASSET])
    def archive_and_cleanup(validated, upsert_results, s3_keys_dict):
        """
        Archive successfully processed files and cleanup temp Parquet files.
        
        Args:
            validated: Original validation result with file metadata
            upsert_results: List of upsert results
            s3_keys_dict: Dict of temp Parquet S3 keys to cleanup
            
        Returns:
            Summary dict with archive and cleanup info
        """
        # Build processed files list from validated data
        valid_files = validated.get("valid", [])
        processed_files = []
        
        # Mark all valid files as successfully processed
        for f in valid_files:
            processed_files.append({
                "original_path": f.get("file"),
                "batch_id": f.get("batch_id"),
                "status": "success"
            })
        
        # Create merge result summary from upsert results
        merge_result = {
            "total_records": sum(r.get("records", 0) for r in upsert_results),
            "types_processed": [r.get("type") for r in upsert_results if r.get("records", 0) > 0]
        }
        
        # Archive processed files
        archive_summary = archive_processed_files(processed_files, merge_result)
        logging.info(f"✅ Archive complete: {archive_summary}")
        
        # Cleanup temporary Parquet files
        deleted_count = 0
        if isinstance(s3_keys_dict, dict) and s3_keys_dict:
            s3_keys = list(s3_keys_dict.values())
            deleted_count = cleanup_temp_parquet_files(s3_keys)
            logging.info(f"🗑️ Cleaned up {deleted_count} temp Parquet files")
        
        return {
            "archive_summary": archive_summary,
            "archived_count": len(processed_files),
            "temp_files_deleted": deleted_count
        }

    # -----------------------------
    # Task Group for parallel chunk processing
    # -----------------------------
    @task_group
    def process_chunks_parallel(file_type_val, filtered_files):
        """
        Process chunks for a single file type in parallel.
        Each chunk is transformed independently - NO JOINS here.
        
        Flow:
        1. Create chunks from filtered files
        2. Transform each chunk in parallel
        3. Return list of transformed chunks
        """
        chunks = create_chunks(filtered_files, file_type_val)
        transformed = transform_single_chunk.expand(chunk_data=chunks)
        return transformed

    # -----------------------------
    # DAG Flow
    # -----------------------------
    
    # 1. Setup Iceberg tables
    setup = setup_iceberg()
    
    # 2. Validate all incoming files
    validated = validate_files(setup_result=setup)
    typed = split_by_type(validated)
    
    # 3. Define file types to process
    types = ["customers", "terminals", "transactions", "travel_profiles"]
    
    # 4. Filter files by type
    filtered = {t: filter_by_type(typed, t) for t in types}
    
    # 5. Process each type's chunks in parallel
    transformed_by_type = [
        process_chunks_parallel(t, filtered[t]) 
        for t in types
    ]
    
    # 6. Combine chunks by type
    combined_dfs = combine_chunks_by_type(transformed_by_type)
    
    # 7. Perform joins across types
    master_dfs = join_master_data(combined_dfs)
    
    # 8. Write master DataFrames to Parquet (avoid XCom limits)
    s3_keys = write_to_parquet(master_dfs, validated)
    
    # 9. Upsert each type to Iceberg (reads from Parquet, converts to PyArrow inline)
    types_to_upsert = ["customers", "terminals", "travel_profiles", "transactions"]
    upsert_results = [upsert_single_type(s3_keys, t) for t in types_to_upsert]
    
    # 10. Archive processed files and cleanup temp Parquet files
    archive_and_cleanup(validated, upsert_results, s3_keys)


# Instantiate the DAG
dag_instance = creditcardfraud_dag()