from airflow.sdk import dag, task, Asset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from pendulum import datetime, now
import logging

# -----------------------------
# Buckets & Config
# -----------------------------
RAW_BUCKET = "raw-data"
STAGING_BUCKET = "staging-data"
ARCHIVE_BUCKET = "archive-data"

MAX_BATCH_SIZE = int(Variable.get("CREDITCARD_MAX_BATCH_SIZE", default_var=10))
BATCH_WINDOW = int(Variable.get("CREDITCARD_BATCH_WINDOW", default_var=60))
CHUNK_SIZE = int(Variable.get("CREDITCARD_CHUNK_SIZE", default_var=4))

# -----------------------------
# Asset definitions
# -----------------------------
RAW_CSV_ASSET = Asset("s3://raw-data/creditcard_files")
STAGED_CSV_ASSET = Asset("s3://staging-data/creditcard_files")
PROCESSED_ASSET = Asset("s3://archive-data/processed_creditcard_files")

# -----------------------------
# Helper functions
# -----------------------------
def chunk_list(lst, n):
    """Split list into chunks of size n"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# -----------------------------
# DAG
# -----------------------------
@dag(
    start_date=datetime(2025, 9, 18),
    schedule=[RAW_CSV_ASSET],  # Schedule on raw CSV asset updates
    catchup=False,
    tags=["minio", "asset", "creditcardfraud", "staging"],
)
def creditcardfrauddetection_dag():

    @task(outlets=[STAGED_CSV_ASSET])
    def stage_files(triggering_asset_events):
        """Move new raw files → staging bucket using asset events"""
        hook = S3Hook(aws_conn_id="minio_s3_conn")
        staged_files = []

        if not triggering_asset_events:
            logging.warning("No triggering asset events for this run")
            return []

        # Process each asset that triggered this DAG run
        for asset, asset_events in triggering_asset_events.items():
            for event in asset_events:
                # Extract file information from the asset event
                file_info = event.extra or {}
                file_key = file_info.get('file_key', f'file_{event.timestamp}')
                
                logging.info(f"Staging {RAW_BUCKET}/{file_key} → {STAGING_BUCKET}/{file_key}")

                try:
                    # Copy to staging bucket
                    hook.copy_object(
                        source_bucket_key=file_key,
                        dest_bucket_key=file_key,
                        source_bucket_name=RAW_BUCKET,
                        dest_bucket_name=STAGING_BUCKET,
                    )
                    # Delete original file
                    hook.delete_objects(bucket=RAW_BUCKET, keys=[file_key])
                    
                    staged_files.append({
                        "path": file_key, 
                        "timestamp": now().int_timestamp,
                        "source_dag": event.source_dag_id,
                        "source_run": event.source_run_id
                    })
                    
                except Exception as e:
                    logging.error(f"Failed to stage file {file_key}: {e}")
                    continue

        logging.info(f"Successfully staged {len(staged_files)} files")
        return staged_files

    @task
    def collect_staged_files(staged_files_list: list):
        """Collect files that are ready for processing based on batch window"""
        if not staged_files_list:
            logging.warning("No staged files to collect")
            return []
            
        cutoff = now().int_timestamp - BATCH_WINDOW
        eligible = [f["path"] for f in staged_files_list if f["timestamp"] <= cutoff]
        
        if not eligible:
            logging.warning("No files ready yet (inside batch window)")
            return []
            
        limited = eligible[:MAX_BATCH_SIZE]
        logging.info(f"Collected {len(limited)} files for batch: {limited}")
        return limited

    @task
    def create_chunks(file_paths: list):
        """Split file paths into processing chunks"""
        if not file_paths:
            return []
        chunks = list(chunk_list(file_paths, CHUNK_SIZE))
        logging.info(f"Created {len(chunks)} chunks from {len(file_paths)} files")
        return chunks

    @task
    def process_file_batch(file_batch: list):
        """Process a batch of files (placeholder for actual processing logic)"""
        if not file_batch:
            return []
            
        processed = []
        hook = S3Hook(aws_conn_id="minio_s3_conn")
        
        for file_path in file_batch:
            try:
                logging.info(f"Processing {STAGING_BUCKET}/{file_path}")
                
                # Placeholder: Add your actual file processing logic here
                # For example: read CSV, validate, clean data, etc.
                
                processed.append({
                    "original_path": file_path,
                    "processed_path": f"processed_{file_path}",
                    "status": "success"
                })
                
            except Exception as e:
                logging.error(f"Failed to process file {file_path}: {e}")
                processed.append({
                    "original_path": file_path,
                    "processed_path": None,
                    "status": "failed",
                    "error": str(e)
                })
                
        logging.info(f"Processed {len([p for p in processed if p['status'] == 'success'])} files successfully")
        return processed

    @task
    def transform_data(processed_files: list):
        """Transform processed data (placeholder for actual transformation logic)"""
        if not processed_files:
            return []
            
        transformed = []
        for file_info in processed_files:
            if file_info['status'] == 'success':
                # Placeholder: Add your transformation logic here
                transformed.append({
                    "original_path": file_info["original_path"],
                    "transformed_path": f"transformed_{file_info['processed_path']}",
                    "records_processed": 1000,  # placeholder count
                })
                
        logging.info(f"Transformed {len(transformed)} files")
        return transformed

    @task
    def merge_transformed(chunks: list):
        """Merge all transformed chunks into final result"""
        if not chunks:
            logging.warning("No chunks to merge")
            return "No data to merge"
            
        # Flatten the list of chunks
        all_files = [file_info for chunk in chunks for file_info in chunk if chunk]
        total_records = sum(f.get("records_processed", 0) for f in all_files)
        
        logging.info(f"Merged {len(chunks)} chunks with {len(all_files)} files into {total_records} total records")
        return {
            "chunks_merged": len(chunks),
            "files_processed": len(all_files),
            "total_records": total_records,
            "status": "completed"
        }

    @task(outlets=[PROCESSED_ASSET])
    def cleanup(processed_batches: list, merge_result):
        """Archive processed files and cleanup staging area"""
        if not processed_batches:
            logging.warning("No processed batches to cleanup")
            return "Nothing to cleanup"
            
        hook = S3Hook(aws_conn_id="minio_s3_conn")
        archived_count = 0
        
        for batch in processed_batches:
            if not batch:
                continue
                
            for file_info in batch:
                if file_info['status'] != 'success':
                    continue
                    
                original_path = file_info['original_path']
                
                try:
                    logging.info(f"Archiving {STAGING_BUCKET}/{original_path} → {ARCHIVE_BUCKET}/{original_path}")
                    
                    # Copy to archive bucket
                    hook.copy_object(
                        source_bucket_key=original_path,
                        dest_bucket_key=original_path,
                        source_bucket_name=STAGING_BUCKET,
                        dest_bucket_name=ARCHIVE_BUCKET,
                    )
                    
                    # Delete from staging
                    hook.delete_objects(bucket=STAGING_BUCKET, keys=[original_path])
                    archived_count += 1
                    
                except Exception as e:
                    logging.error(f"Failed to archive file {original_path}: {e}")
                    continue
        
        result = f"Successfully archived {archived_count} files. Merge result: {merge_result}"
        logging.info(result)
        return result

    # -----------------------------
    # DAG flow with proper task dependencies
    # -----------------------------
    staged = stage_files()
    files = collect_staged_files(staged)
    chunks = create_chunks(files)
    
    # Use dynamic task mapping for parallel processing
    processed = process_file_batch.expand(file_batch=chunks)
    transformed = transform_data.expand(processed_files=processed)
    
    # Merge results
    merge_result = merge_transformed(transformed)
    
    # Cleanup with both processed batches and merge result
    cleanup(processed, merge_result)


# Instantiate the DAG
dag_instance = creditcardfrauddetection_dag()