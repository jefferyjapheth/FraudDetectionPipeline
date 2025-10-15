"""
Credit Card Fraud Detection ETL Pipeline
=========================================

Purpose:
    Processes raw credit card transaction data from Ghana-based fraud simulator
    and loads it into Iceberg tables with SCD Type 2 dimension tracking.

Architecture:
    - Source: MinIO S3 (raw CSV files)
    - Processing: Parallel chunk-based transformation
    - Storage: Iceberg tables with SCD2 versioning for dimensions
    - Coordination: Airflow 3.0 with asset-based triggering

Data Model:
    Dimensions (SCD2):
        - dim_customers: Customer profiles with location and behavior metrics
        - dim_terminals: Terminal locations with geographic coordinates
    
    Facts (append-only):
        - fact_transactions: Transaction events (pre-enriched by simulator)
        - dim_travel_profiles: Customer travel patterns
    
Performance Considerations:
    - Parallel chunk processing for large files
    - Parquet intermediate storage to avoid XCom size limits
    - Early SCD2 field initialization in transform phase
    - Schema enforcement before Iceberg upsert

Author: Data Engineering Team
Version: 2.0 (SCD2-enabled)
"""

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
from src.transform.iceberg_manager import initialize_iceberg_tables, upsert_to_iceberg_with_scd2


# Asset definitions for lineage tracking
RAW_CSV_ASSET = Asset("s3://raw-data/creditcard_files")
ICEBERG_ASSET = Asset("iceberg://master_data/tables")
PROCESSED_ASSET = Asset("s3://archive-data/processed_creditcard_files")


@dag(
    start_date=datetime(2025, 9, 18),
    schedule=[RAW_CSV_ASSET],
    catchup=False,
    tags=["minio", "iceberg", "creditcardfraud", "etl", "parallel", "parquet", "scd2"],
    max_active_runs=1,
)
def creditcardfraud_dag():
    """
    Main ETL pipeline for credit card fraud detection data.
    
    Pipeline Flow:
        1. Initialize Iceberg table schemas
        2. Validate incoming CSV files against expected schemas
        3. Partition files by entity type (customers, terminals, transactions, travel_profiles)
        4. Transform data in parallel chunks with SCD2 field initialization
        5. Combine transformed chunks by entity type
        6. Validate data completeness (no joins - transactions pre-enriched)
        7. Write consolidated data to Parquet (intermediate storage)
        8. Upsert to Iceberg with SCD2 version control for dimensions
        9. Archive processed files and cleanup temporary artifacts
    
    SCD2 Implementation:
        - Tracks historical changes for customer and terminal dimensions
        - Maintains version history with effective_date and end_date
        - Preserves current/historical flags for time-travel queries
    """

    @task
    def setup_iceberg():
        """
        Initialize Iceberg table schemas if they don't exist.
        
        Creates four tables:
            - dim_customers: Customer dimension with SCD2 tracking
            - dim_terminals: Terminal dimension with SCD2 tracking
            - fact_transactions: Transaction facts (append-only)
            - dim_travel_profiles: Customer travel patterns (append-only)
        
        Returns:
            dict: Status indicator for downstream task dependencies
        """
        initialize_iceberg_tables()
        return {"status": "initialized"}

    @task
    def validate_files(setup_result, triggering_asset_events=None):
        """
        Validate incoming CSV files against expected schemas.
        
        Process:
            1. Read batch metadata from S3 (metadata_latest.json)
            2. For each file, determine entity type from filename pattern
            3. Validate schema structure and data types
            4. Separate valid files from invalid (schema violations, malformed data)
            5. Move invalid files to archive/invalid/ prefix
        
        Args:
            setup_result: Dependency on Iceberg setup completion
            triggering_asset_events: Asset event metadata (auto-injected by Airflow)
        
        Returns:
            dict: {
                "valid": List of valid file metadata dicts,
                "invalid": List of invalid file metadata dicts,
                "batch_id": UUID for this processing batch
            }
        """
        hook = S3Hook(aws_conn_id="minio_default")
        valid_files, invalid_files = [], []

        # Attempt to load batch metadata
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

        # Validate each file
        for file_key in file_list:
            ftype = file_type(file_key)
            
            # Reject files with unrecognized patterns or non-CSV extensions
            if not ftype or not file_key.endswith(".csv"):
                move_file(hook, RAW_BUCKET, ARCHIVE_BUCKET, file_key, f"invalid/{file_key}")
                invalid_files.append({"file": file_key, "batch_id": batch_id})
                continue

            # Schema validation (column presence, data types, constraints)
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

        logging.info(f"Validation complete: {len(valid_files)} valid, {len(invalid_files)} invalid")
        return {"valid": valid_files, "invalid": invalid_files, "batch_id": batch_id}

    @task
    def split_by_type(valid_dict):
        """
        Extract list of valid files from validation result dictionary.
        
        Args:
            valid_dict: Output from validate_files task
        
        Returns:
            list: Valid file metadata dictionaries
        """
        return valid_dict.get("valid", [])

    @task
    def filter_by_type(files, target_type):
        """
        Filter files by entity type for parallel processing streams.
        
        Args:
            files: List of all valid file metadata
            target_type: Entity type to filter (customers, terminals, transactions, travel_profiles)
        
        Returns:
            list: Filtered file metadata for specified type
        """
        filtered = [f for f in files if f["type"] == target_type]
        logging.info(f"Filtered {len(filtered)} files for type: {target_type}")
        return filtered

    @task
    def create_chunks(files, file_type_val):
        """
        Partition files into chunks for parallel processing.
        
        Strategy:
            - Small files (< 10k rows): Process together in single chunk
            - Large files (> 10k rows): Split into multiple chunks
            - Configurable chunk size in chunking.create_chunks_safe()
        
        Args:
            files: List of file metadata for a single entity type
            file_type_val: Entity type being chunked
        
        Returns:
            list: Chunk metadata dictionaries with file assignments
        """
        if not files:
            logging.info(f"No files to chunk for type: {file_type_val}")
            return []
        
        chunks = create_chunks_safe(files)
        
        # Add tracking metadata to each chunk
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

    @task
    def transform_single_chunk(chunk_data):
        """
        Transform a single chunk of files in parallel.
        
        Transformations Applied:
            1. Read CSV data from S3
            2. Add batch tracking columns (BATCH_ID, created_at)
            3. For dimensions (customers, terminals):
               - Initialize SCD2 tracking fields (version, effective_date, end_date, is_current)
            4. For transactions:
               - Parse and validate TX_DATETIME timestamps
            5. Serialize to dict format for XCom transfer
        
        Note: No cross-entity joins occur here - each chunk is independent.
              Transactions are already enriched by the simulator with dimension attributes.
        
        Args:
            chunk_data: Dict containing chunk_id, type, and list of file metadata
        
        Returns:
            dict: Serialized DataFrame with metadata (or None if chunk empty/failed)
        """
        if not chunk_data or not chunk_data.get("files"):
            logging.warning(f"Empty or invalid chunk received: {chunk_data}")
            return None
        
        hook = S3Hook(aws_conn_id="minio_default")
        file_type_val = chunk_data.get("type")
        files = chunk_data.get("files", [])
        chunk_id = chunk_data.get("chunk_id", "unknown")
        
        logging.info(f"Processing chunk {chunk_id} with {len(files)} files")
        
        try:
            all_data = []
            file_keys = []
            
            # Read all files in this chunk
            for f in files:
                file_key = f.get("file")
                batch_id = f.get("batch_id")
                
                try:
                    obj = hook.get_key(file_key, bucket_name=RAW_BUCKET)
                    csv_data = obj.get()["Body"].read().decode("utf-8")
                    df = pd.read_csv(StringIO(csv_data))
                    
                    # Add lineage tracking columns
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
            
            # Apply type-specific transformations
            if file_type_val in ["customers", "terminals"]:
                # Initialize SCD2 tracking fields early
                # This is critical - fields must exist before SCD2 logic in upsert phase
                if "is_current" not in df_combined.columns:
                    df_combined["is_current"] = True
                if "end_date" not in df_combined.columns:
                    df_combined["end_date"] = pd.NaT
                if "version" not in df_combined.columns:
                    df_combined["version"] = 1
                if "effective_date" not in df_combined.columns:
                    df_combined["effective_date"] = pd.Timestamp.utcnow()
                
                logging.info(f"Added SCD2 tracking fields to {file_type_val} chunk")
                    
            elif file_type_val == "transactions":
                # Normalize transaction timestamps to UTC
                if "TX_DATETIME" in df_combined.columns:
                    df_combined["TX_DATETIME"] = pd.to_datetime(df_combined["TX_DATETIME"], utc=True)
            
            logging.info(f"Chunk {chunk_id} transformed: {len(df_combined)} records")
            
            # Serialize datetime columns to strings for XCom transfer
            # (XCom cannot handle datetime64 objects directly)
            datetime_cols = df_combined.select_dtypes(
                include=['datetime64[ns, UTC]', 'datetime64[ns]', 'datetime64']
            ).columns
            for col in datetime_cols:
                df_combined[col] = df_combined[col].astype(str)
            
            # Return serialized data with metadata
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
            logging.error(f"Error transforming chunk {chunk_id}: {e}")
            raise

    @task
    def combine_chunks_by_type(transformed_chunks_list):
        """
        Combine parallel-processed chunks back into single DataFrames per entity type.
        
        Process:
            1. Flatten nested list structure from parallel task groups
            2. Group chunks by entity type
            3. Concatenate DataFrames for each type
            4. Reconvert datetime columns from serialized strings
        
        Args:
            transformed_chunks_list: Nested list of chunk results from parallel processing
        
        Returns:
            dict: {entity_type: DataFrame} mapping for all processed types
        """
        # Flatten nested structure from multiple parallel task groups
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
        
        logging.info(f"Combining {len(all_chunks)} total chunks across all types")
        
        # Group chunks by entity type
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
                logging.info(f"Combined {file_type_val}: {len(combined_df)} records")
        
        return combined_dfs

    @task
    def join_master_data(combined_dfs):
        """
        Validate and pass through entity data without performing joins.
        
        Design Rationale:
            The fraud simulator pre-enriches transaction records with all necessary
            dimension attributes during generation. This includes:
            
            - TERMINAL_CITY, TERMINAL_REGION (from terminals dimension)
            - CUSTOMER_TRAVEL_REGIONS (from travel_profiles dimension)
            - DISTANCE_FROM_HOME_KM (calculated from customer coordinates)
            - AVG_REGION_DISTANCE_KM (pre-computed regional distances)
            - IS_NEW_REGION (derived from travel pattern analysis)
            
            Therefore, this pipeline's role is SCD2 version control and data quality
            validation, not star schema materialization. Joining here would be
            redundant and would add columns that get dropped during schema enforcement.
        
        Validation Performed:
            - Check for empty DataFrames
            - Verify required enrichment columns present in transactions
            - Log record counts and unique key counts for monitoring
        
        Args:
            combined_dfs: Dict mapping entity types to their consolidated DataFrames
        
        Returns:
            dict: Same structure as input, validated and ready for Parquet storage
        """
        if not combined_dfs:
            logging.warning("No data to process")
            return {}
        
        logging.info(f"Processing {len(combined_dfs)} data types (pass-through mode)")
        
        # Validate data presence and pass through
        master_dfs = {}
        
        for data_type in ["customers", "terminals", "travel_profiles", "transactions"]:
            if data_type in combined_dfs:
                df = combined_dfs[data_type]
                
                # Skip empty DataFrames
                if df.empty:
                    logging.warning(f"DataFrame for {data_type} is empty, skipping")
                    continue
                
                # Log statistics for monitoring
                row_count = len(df)
                col_count = len(df.columns)
                
                # Type-specific validation and logging
                if data_type == "customers":
                    key_col = "CUSTOMER_ID"
                    unique_count = df[key_col].nunique()
                    logging.info(f"{data_type}: {row_count} records, {unique_count} unique customers, {col_count} columns")
                    
                elif data_type == "terminals":
                    key_col = "TERMINAL_ID"
                    unique_count = df[key_col].nunique()
                    logging.info(f"{data_type}: {row_count} records, {unique_count} unique terminals, {col_count} columns")
                    
                elif data_type == "travel_profiles":
                    key_col = "CUSTOMER_ID"
                    unique_count = df[key_col].nunique()
                    logging.info(f"{data_type}: {row_count} records, {unique_count} unique profiles, {col_count} columns")
                    
                elif data_type == "transactions":
                    # Verify transactions have required enrichment from simulator
                    required_enrichments = [
                        "TERMINAL_CITY", 
                        "TERMINAL_REGION",
                        "CUSTOMER_TRAVEL_REGIONS"
                    ]
                    missing_enrichments = [col for col in required_enrichments if col not in df.columns]
                    
                    if missing_enrichments:
                        logging.warning(f"Transactions missing enrichment columns: {missing_enrichments}")
                    else:
                        logging.info(f"{data_type}: {row_count} records, {col_count} columns (pre-enriched by simulator)")
                
                # Add to master DataFrames collection
                master_dfs[data_type] = df
            else:
                logging.info(f"{data_type} not in batch, skipping")
        
        logging.info(f"Processed {len(master_dfs)} data types (transactions pre-enriched, no joins needed)")
        
        # Summary statistics for monitoring
        total_records = sum(len(df) for df in master_dfs.values())
        logging.info(f"Total records across all types: {total_records:,}")
        
        return master_dfs

    @task
    def write_to_parquet(master_dfs, validated):
        """
        Write consolidated DataFrames to Parquet files in MinIO.
        
        Purpose:
            Airflow XCom has size limits (default 48KB) that cannot accommodate
            large DataFrames. Writing to Parquet provides:
            - Efficient columnar storage for analytical queries
            - Compression (typically 80-90% reduction)
            - Schema preservation
            - Fast read/write operations
        
        The S3 keys returned replace large DataFrame objects in XCom.
        
        Args:
            master_dfs: Dict mapping entity types to DataFrames
            validated: Validation result containing batch_id for file naming
        
        Returns:
            dict: {entity_type: s3_key} mapping for downstream tasks
        """
        if not isinstance(master_dfs, dict):
            logging.error(f"Expected dict, got {type(master_dfs)}")
            return {}
        
        if not master_dfs:
            logging.warning("No master DataFrames to write")
            return {}
        
        # Extract batch ID for file naming and lineage tracking
        batch_id = validated.get("batch_id", "unknown_batch")
        
        hook = S3Hook(aws_conn_id="minio_default")
        s3_keys = write_multiple_dataframes(master_dfs, batch_id, hook)
        
        logging.info(f"Wrote {len(s3_keys)} DataFrames to Parquet in MinIO")
        return s3_keys

    @task(outlets=[ICEBERG_ASSET])
    def upsert_single_type(s3_keys_dict, file_type_val):
        """
        Upsert a single entity type to its corresponding Iceberg table.
        
        SCD2 Logic Application:
            For dimensions (customers, terminals):
                1. Read existing records from Iceberg
                2. Compare incoming records against current versions
                3. For changed records:
                   - Close old version (set is_current=False, end_date=now)
                   - Insert new version (increment version, set effective_date=now)
                4. For new records: Insert with version=1, is_current=True
                5. For unchanged records: Skip (no duplicate insertion)
            
            For facts (transactions, travel_profiles):
                - Simple append (no version tracking needed)
        
        Schema Enforcement:
            - prepare_for_iceberg_with_arrow() validates all columns
            - Drops any extra columns not in Iceberg schema
            - Converts to PyArrow Table with proper nullable settings
        
        Args:
            s3_keys_dict: Dict mapping entity types to Parquet S3 keys
            file_type_val: Entity type to process (customers, terminals, etc.)
        
        Returns:
            dict: {type: str, records: int, status: str} result summary
        """
        if not isinstance(s3_keys_dict, dict):
            logging.error(f"Expected dict, got {type(s3_keys_dict)}")
            return {"type": file_type_val, "records": 0, "status": "failed"}
        
        if file_type_val not in s3_keys_dict:
            logging.warning(f"Type {file_type_val} not in s3_keys_dict")
            return {"type": file_type_val, "records": 0, "status": "skipped"}
        
        s3_key = s3_keys_dict[file_type_val]
        
        try:
            # Read DataFrame from Parquet intermediate storage
            hook = S3Hook(aws_conn_id="minio_default")
            df = read_dataframe_from_parquet(s3_key, hook)
            
            if df.empty:
                logging.info(f"Skipping empty DataFrame for type: {file_type_val}")
                return {"type": file_type_val, "records": 0, "status": "empty"}
            
            logging.info(f"Preparing {file_type_val} for Iceberg: {len(df)} records, {len(df.columns)} columns")
            
            # Prepare DataFrame with proper schema and convert to PyArrow
            from src.transform.transformation import prepare_for_iceberg_with_arrow
            arrow_table = prepare_for_iceberg_with_arrow(df, file_type=file_type_val)
            
            logging.info(f"Upserting {len(arrow_table)} records for type: {file_type_val}")
            
            # Upsert to Iceberg with SCD2 logic (automatically applied for dimensions)
            result = upsert_to_iceberg_with_scd2(file_type_val, arrow_table)
            logging.info(f"Upserted {result.get('records', 0)} records for {file_type_val} (status: {result.get('status')})")
            
            return result
            
        except Exception as e:
            logging.error(f"Failed to upsert {file_type_val}: {e}")
            raise

    @task(outlets=[PROCESSED_ASSET])
    def archive_and_cleanup(validated, upsert_results, s3_keys_dict):
        """
        Archive successfully processed files and cleanup temporary artifacts.
        
        Archival Process:
            1. Move processed CSV files from raw-data bucket to archive-data bucket
            2. Organize by processing date for audit trail
            3. Retain for compliance and reprocessing scenarios
        
        Cleanup Process:
            1. Delete temporary Parquet files from master-data/temp/ prefix
            2. Prevents storage bloat from intermediate artifacts
            3. Parquet files only needed between write_to_parquet and upsert tasks
        
        Args:
            validated: Original validation result with file metadata
            upsert_results: List of upsert result dicts from parallel processing
            s3_keys_dict: Dict of temporary Parquet S3 keys to delete
        
        Returns:
            dict: Summary with archive counts and cleanup statistics
        """
        # Build list of successfully processed files
        valid_files = validated.get("valid", [])
        processed_files = []
        
        for f in valid_files:
            processed_files.append({
                "original_path": f.get("file"),
                "batch_id": f.get("batch_id"),
                "status": "success"
            })
        
        # Create summary for archival metadata
        merge_result = {
            "total_records": sum(r.get("records", 0) for r in upsert_results),
            "types_processed": [r.get("type") for r in upsert_results if r.get("records", 0) > 0]
        }
        
        # Archive processed files to dated folders
        archive_summary = archive_processed_files(processed_files, merge_result)
        logging.info(f"Archive complete: {archive_summary}")
        
        # Cleanup temporary Parquet files
        deleted_count = 0
        if isinstance(s3_keys_dict, dict) and s3_keys_dict:
            s3_keys = list(s3_keys_dict.values())
            deleted_count = cleanup_temp_parquet_files(s3_keys)
            logging.info(f"Cleaned up {deleted_count} temporary Parquet files")
        
        return {
            "archive_summary": archive_summary,
            "archived_count": len(processed_files),
            "temp_files_deleted": deleted_count
        }

    @task_group
    def process_chunks_parallel(file_type_val, filtered_files):
        """
        Task group for parallel chunk processing of a single entity type.
        
        Dynamic Task Mapping:
            Uses Airflow's expand() operator to create one task instance per chunk.
            Allows horizontal scaling based on data volume.
        
        Flow:
            1. create_chunks: Partition files into optimal chunk sizes
            2. transform_single_chunk.expand(): Process chunks in parallel
            3. Return: List of transformed chunk results
        
        Args:
            file_type_val: Entity type being processed
            filtered_files: Files filtered to this entity type
        
        Returns:
            list: Transformed chunk results for combining
        """
        chunks = create_chunks(filtered_files, file_type_val)
        transformed = transform_single_chunk.expand(chunk_data=chunks)
        return transformed

    # ========================================
    # DAG Execution Flow
    # ========================================
    
    # Step 1: Initialize Iceberg table schemas
    setup = setup_iceberg()
    
    # Step 2: Validate incoming files and extract metadata
    validated = validate_files(setup_result=setup)
    typed = split_by_type(validated)
    
    # Step 3: Define entity types to process
    types = ["customers", "terminals", "transactions", "travel_profiles"]
    
    # Step 4: Filter files by entity type for parallel streams
    filtered = {t: filter_by_type(typed, t) for t in types}
    
    # Step 5: Process each type's chunks in parallel (horizontal scaling)
    transformed_by_type = [
        process_chunks_parallel(t, filtered[t]) 
        for t in types
    ]
    
    # Step 6: Combine parallel chunks back into single DataFrames per type
    combined_dfs = combine_chunks_by_type(transformed_by_type)
    
    # Step 7: Validate data completeness (no joins - transactions pre-enriched)
    master_dfs = join_master_data(combined_dfs)
    
    # Step 8: Write to Parquet intermediate storage (avoid XCom limits)
    s3_keys = write_to_parquet(master_dfs, validated)
    
    # Step 9: Upsert each type to Iceberg with SCD2 version control
    types_to_upsert = ["customers", "terminals", "travel_profiles", "transactions"]
    upsert_results = [upsert_single_type(s3_keys, t) for t in types_to_upsert]
    
    # Step 10: Archive processed files and cleanup temporary artifacts
    archive_and_cleanup(validated, upsert_results, s3_keys)


# Instantiate the DAG for Airflow scheduler
dag_instance = creditcardfraud_dag()