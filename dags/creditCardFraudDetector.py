from airflow.sdk import dag, task, Asset, task_group
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging
import json
import pandas as pd
from io import StringIO

from src.config.settings import RAW_BUCKET, ARCHIVE_BUCKET
from src.validate.schema_validation import file_type, validate_csv_schema
from src.validate.chunking import create_chunks_safe
from src.extract.s3_operations import move_file
from src.transform.iceberg_upsert import upsert_to_iceberg, initialize_iceberg_tables
from src.transform.transformation import transform_and_join

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
    tags=["minio", "iceberg", "creditcardfraud", "etl", "parallel"],
    max_active_runs=1,
)
def creditcardfraud_dag():

    # -----------------------------
    # Initialize Iceberg tables
    # -----------------------------
    @task
    def setup_iceberg():
        """Initialize Iceberg tables before processing"""
        initialize_iceberg_tables()
        return {"status": "initialized"}

    # -----------------------------
    # Validate files
    # -----------------------------
    @task
    def validate_files(setup_result, triggering_asset_events=None):
        """Validate incoming CSV files and separate valid from invalid"""
        hook = S3Hook(aws_conn_id="minio_default")
        valid_files, invalid_files = [], []

        metadata_key = "metadata/metadata_latest.json"
        try:
            obj = hook.get_key(metadata_key, bucket_name=RAW_BUCKET)
            metadata = json.loads(obj.get()["Body"].read())
            batch_id = metadata.get("batch_id")
            file_list = [f["s3_path"].replace(f"s3://{RAW_BUCKET}/", "") for f in metadata.get("files", [])]
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

            validation_result = validate_csv_schema(hook, RAW_BUCKET, file_key, ftype)
            if validation_result.get("valid", False):
                valid_files.append({"file": file_key, "type": ftype, "batch_id": batch_id})
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
        
        # create_chunks_safe returns List[List[Dict]]
        chunks = create_chunks_safe(files)
        
        # Add metadata to each chunk for tracking
        formatted_chunks = []
        for idx, chunk in enumerate(chunks):
            formatted_chunks.append({
                "chunk_id": f"{file_type_val}_chunk_{idx}",
                "type": file_type_val,
                "files": chunk,  # List of file dicts
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
        Reads files from S3, applies transformations, returns transformed data.
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
            # Read all files in this chunk
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
            
            # Apply type-specific transformations (SCD2 handling, dtype conversions)
            if file_type_val in ["customers", "terminals"]:
                df_combined["is_current"] = True
                df_combined["end_date"] = pd.NaT
                df_combined["effective_date"] = pd.to_datetime(df_combined["effective_date"], utc=True)
                df_combined["created_at"] = pd.to_datetime(df_combined["created_at"], utc=True)
                
            elif file_type_val == "travel_profiles":
                df_combined["created_at"] = pd.to_datetime(df_combined["created_at"], utc=True)
                
            elif file_type_val == "transactions":
                df_combined["created_at"] = pd.to_datetime(df_combined["created_at"], utc=True)
                if "TX_DATETIME" in df_combined.columns:
                    df_combined["TX_DATETIME"] = pd.to_datetime(df_combined["TX_DATETIME"], utc=True)
            
            logging.info(f"✅ Chunk {chunk_id} transformed: {len(df_combined)} records")
            
            # Convert datetime columns to strings for XCom serialization
            datetime_cols = df_combined.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]']).columns
            for col in datetime_cols:
                df_combined[col] = df_combined[col].astype(str)
            
            # Serialize DataFrame for XCom
            return {
                "chunk_id": chunk_id,
                "type": file_type_val,
                "data": df_combined.to_dict('records'),
                "columns": list(df_combined.columns),
                "datetime_columns": list(datetime_cols),  # Track which cols need reconversion
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
        # Flatten the nested structure from multiple task groups
        all_chunks = []
        
        # Handle LazyXComSequence and nested lists
        if hasattr(transformed_chunks_list, '__iter__'):
            for item in transformed_chunks_list:
                if item is None:
                    continue
                # If it's a list or sequence, iterate through it
                if hasattr(item, '__iter__') and not isinstance(item, dict):
                    for chunk in item:
                        if chunk is not None:
                            all_chunks.append(chunk)
                else:
                    # Single chunk dict
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
            
            # Reconstruct DataFrames from serialized data
            type_dfs = []
            datetime_cols = set()
            
            for chunk in type_chunks:
                df = pd.DataFrame(chunk["data"])
                
                # Track datetime columns that need reconversion
                if "datetime_columns" in chunk:
                    datetime_cols.update(chunk["datetime_columns"])
                
                type_dfs.append(df)
            
            if type_dfs:
                combined_df = pd.concat(type_dfs, ignore_index=True)
                
                # Reconvert datetime columns from strings back to datetime
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
        This joins transactions with customers, terminals, and travel_profiles.
        
        Args:
            combined_dfs: Dict of DataFrames by type {type: DataFrame}
            
        Returns:
            Dict of master DataFrames ready for Iceberg upsert
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
                df_tx = df_tx.merge(
                    df_customers[["CUSTOMER_ID", "HOME_CITY", "HOME_REGION"]],
                    on="CUSTOMER_ID",
                    how="left"
                )
                logging.info(f"✅ Joined with customers: {len(df_tx)} records")
            
            # Join with terminals (left join to keep all transactions)
            if "terminals" in combined_dfs:
                df_terminals = combined_dfs["terminals"]
                df_tx = df_tx.merge(
                    df_terminals[["TERMINAL_ID", "CITY", "REGION"]],
                    left_on="TERMINAL_ID",
                    right_on="TERMINAL_ID",
                    how="left",
                    suffixes=("", "_terminal")
                )
                logging.info(f"✅ Joined with terminals: {len(df_tx)} records")
            
            # Join with travel profiles (left join, optional enrichment)
            if "travel_profiles" in combined_dfs:
                df_travel = combined_dfs["travel_profiles"]
                df_tx = df_tx.merge(
                    df_travel[["CUSTOMER_ID", "TRAVEL_REGIONS", "AVG_TRAVEL_DISTANCE_KM"]],
                    on="CUSTOMER_ID",
                    how="left"
                )
                logging.info(f"✅ Joined with travel profiles: {len(df_tx)} records")
            
            master_dfs["transactions"] = df_tx
            logging.info(f"✅ Final transactions DataFrame: {len(df_tx)} records")
        
        logging.info(f"🎉 Created master DataFrames for {len(master_dfs)} types")
        return master_dfs

    # -----------------------------
    # Upsert to Iceberg
    # -----------------------------
    @task(outlets=[ICEBERG_ASSET])
    def upsert_master_dfs(master_dfs):
        """Upsert all master DataFrames to Iceberg tables"""
        results = []
        for t, df in master_dfs.items():
            if df.empty:
                logging.info(f"Skipping empty DataFrame for type: {t}")
                continue
            
            logging.info(f"📤 Upserting {len(df)} records for type: {t}")
            result = upsert_to_iceberg(df=df, file_info={"type": t})
            results.append(result)
            logging.info(f"✅ Upserted {result.get('records', 0)} records for {t}")
        
        return results

    # -----------------------------
    # Archive valid files
    # -----------------------------
    @task(outlets=[PROCESSED_ASSET])
    def archive_files(master_dfs_results):
        """Archive successfully processed files"""
        hook = S3Hook(aws_conn_id="minio_default")
        archived_count = 0
        
        for result in master_dfs_results:
            if result.get("records", 0) > 0:
                file_type_val = result["type"]
                archived_count += result.get("records", 0)
        
        logging.info(f"✅ Archived {archived_count} records")
        return {"archived": archived_count}

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
        # Create chunks with file type
        chunks = create_chunks(filtered_files, file_type_val)
        
        # Transform chunks in parallel using dynamic task mapping
        # .expand() creates one task instance per chunk
        transformed = transform_single_chunk.expand(chunk_data=chunks)
        
        return transformed

    # -----------------------------
    # DAG Flow
    # -----------------------------
    
    # Setup Iceberg tables
    setup = setup_iceberg()
    
    # Validate all incoming files
    validated = validate_files(setup_result=setup)
    typed = split_by_type(validated)
    
    # Define file types to process
    types = ["customers", "terminals", "transactions", "travel_profiles"]
    
    # Filter files by type
    filtered = {t: filter_by_type(typed, t) for t in types}
    
    # Process each type's chunks in parallel (within task groups)
    # Each task group contains: create_chunks → transform_chunks (parallel)
    transformed_by_type = [
        process_chunks_parallel(t, filtered[t]) 
        for t in types
    ]
    
    # AFTER all chunks are transformed, combine them by type
    combined_dfs = combine_chunks_by_type(transformed_by_type)
    
    # THEN perform joins across types (transactions + customers + terminals + travel_profiles)
    master_dfs = join_master_data(combined_dfs)
    
    # Upsert master DataFrames to Iceberg
    upsert_results = upsert_master_dfs(master_dfs)
    
    # Archive processed files
    archive_files(upsert_results)


dag_instance = creditcardfraud_dag()