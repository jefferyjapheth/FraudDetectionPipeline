from airflow.sdk import dag, task, Asset
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging
import json

from src.config.settings import RAW_BUCKET, STAGING_BUCKET, ARCHIVE_BUCKET
from src.validate.schema_validation import file_type, validate_csv_schema
from src.validate.chunking import create_chunks_safe
from src.extract.s3_operations import move_file
from src.transform.processing import process_file_batch
from src.transform.transformation import transform_data
from src.transform.merging import merge_transformed
from src.load.cleanup import cleanup_archives

# -----------------------------
# Assets
# -----------------------------
RAW_CSV_ASSET = Asset("s3://raw-data/creditcard_files")
STAGED_CSV_ASSET = Asset("s3://staging-data/creditcard_files")
PROCESSED_ASSET = Asset("s3://archive-data/processed_creditcard_files")

# -----------------------------
# DAG
# -----------------------------
@dag(
    start_date=datetime(2025, 9, 18),
    schedule=[RAW_CSV_ASSET],
    catchup=False,
    tags=["minio", "asset", "creditcardfraud", "etl"],
)
def creditcardfrauddetection_dag():

    # -----------------------------
    # 1️⃣ Validate files
    # -----------------------------
    @task(outlets=[STAGED_CSV_ASSET])
    def validate_raw_files(triggering_asset_events):
        hook = S3Hook(aws_conn_id="minio_default")
        valid_files, invalid_files = [], []

        # Read metadata_latest.json for batch_id & file paths
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
            logging.warning("No files found in metadata for validation")
            return {"valid": [], "invalid": []}

        for file_key in file_list:
            ftype = file_type(file_key)
            if not ftype or not file_key.endswith(".csv"):
                move_file(hook, RAW_BUCKET, ARCHIVE_BUCKET, file_key, f"raw/{file_key}")
                invalid_files.append({"file": file_key, "batch_id": batch_id})
                continue

            if validate_csv_schema(hook, RAW_BUCKET, file_key, ftype):
                if move_file(hook, RAW_BUCKET, STAGING_BUCKET, file_key):
                    valid_files.append({"file": file_key, "batch_id": batch_id})
            else:
                move_file(hook, RAW_BUCKET, ARCHIVE_BUCKET, file_key, f"raw/{file_key}")
                invalid_files.append({"file": file_key, "batch_id": batch_id})

        logging.info(f"✅ Valid: {len(valid_files)} | ❌ Invalid: {len(invalid_files)}")
        return {"valid": valid_files, "invalid": invalid_files}

    # -----------------------------
    # 2️⃣ Split files by type
    # -----------------------------
    @task
    def split_by_type(valid_invalid_dict):
        valid_files = valid_invalid_dict.get("valid", [])
        return [
            {"type": file_type(f["file"]), "file": f["file"], "batch_id": f.get("batch_id")}
            for f in valid_files
        ]

    @task
    def filter_by_type(typed_files, target_type):
        return [f for f in typed_files if f["type"] == target_type]

    @task
    def create_chunks_task(file_list):
        return create_chunks_safe(file_list)

    # -----------------------------
    # 3️⃣ Process & transform (combined)
    # -----------------------------
    @task
    def process_and_transform_batch(file_batch):
        """
        Process and transform a batch in one step to avoid mapping issues.
        Returns both processed and transformed results.
        """
        if not file_batch:
            return {"processed": [], "transformed": []}
        
        # Process
        processed = process_file_batch(file_batch)
        
        # Transform
        transformed = transform_data(processed)
        
        return {"processed": processed, "transformed": transformed}

    # -----------------------------
    # 4️⃣ Collect results
    # -----------------------------
    @task
    def collect_results(results_list):
        """
        Collect all results from mapped tasks.
        results_list is a list of dicts, each with 'processed' and 'transformed' keys.
        """
        all_processed = []
        all_transformed = []
        
        logging.info(f"Collecting results from {len(results_list)} batches")
        
        for result in results_list:
            if not result:
                continue
            
            processed = result.get("processed", [])
            transformed = result.get("transformed", [])
            
            if isinstance(processed, list):
                all_processed.extend(processed)
            elif processed:
                all_processed.append(processed)
            
            if isinstance(transformed, list):
                all_transformed.extend(transformed)
            elif transformed:
                all_transformed.append(transformed)
        
        logging.info(f"Total collected - Processed: {len(all_processed)}, Transformed: {len(all_transformed)}")
        
        return {"processed": all_processed, "transformed": all_transformed}

  
    # 5️⃣ Merge
    # -----------------------------
    @task
    def merge_task(collected_results):
        transformed = collected_results.get("transformed", [])
        logging.info(f"Merging {len(transformed)} transformed files")
        return merge_transformed(transformed)

    # -----------------------------
    # 6️⃣ Cleanup / archive
    # -----------------------------
    @task(outlets=[PROCESSED_ASSET])
    def cleanup_task(collected_results, merge_result):
        processed = collected_results.get("processed", [])
        logging.info(f"Cleaning up {len(processed)} processed files")
        return cleanup_archives(processed, merge_result)

    # -----------------------------
    # DAG Flow
    # -----------------------------
    validated = validate_raw_files()
    typed = split_by_type(validated)

    types = ["customers", "terminals", "transactions", "travel_profiles"]

    # Filter and chunk by type
    filtered = {t: filter_by_type(typed, t) for t in types}
    chunks = {t: create_chunks_task(filtered[t]) for t in types}

    # Process and transform in one mapped task per type
    results = {t: process_and_transform_batch.expand(file_batch=chunks[t]) for t in types}

    # Collect results from each type
    collected = {t: collect_results(results[t]) for t in types}

    # Combine all types
    @task
    def combine_all_types(customers, terminals, transactions, travel):
        """Combine collected results from all file types."""
        all_processed = []
        all_transformed = []
        
        for name, data in [("customers", customers), ("terminals", terminals), 
                           ("transactions", transactions), ("travel_profiles", travel)]:
            if not data:
                continue
            all_processed.extend(data.get("processed", []))
            all_transformed.extend(data.get("transformed", []))
        
        logging.info(f"Combined totals - Processed: {len(all_processed)}, Transformed: {len(all_transformed)}")
        return {"processed": all_processed, "transformed": all_transformed}

    combined = combine_all_types(
        collected["customers"],
        collected["terminals"],
        collected["transactions"],
        collected["travel_profiles"]
    )

    merge_result = merge_task(combined)
    cleanup_task(combined, merge_result)


dag_instance = creditcardfrauddetection_dag()