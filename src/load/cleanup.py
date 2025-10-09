from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging
from typing import List, Dict, Union
from src.config.settings import RAW_BUCKET, ARCHIVE_BUCKET

def cleanup_archives(collected_results: Union[Dict, List], merge_result: Dict) -> str:
    """
    Archive processed files from raw bucket to archive.

    Args:
        collected_results: Either a dict with 'processed' key or a list of processed file dicts
        merge_result: Summary dict from merge_transformed

    Returns:
        str: Summary message
    """
    hook = S3Hook(aws_conn_id="minio_default")
    archived_count = 0

    # Extract the processed files list
    if isinstance(collected_results, dict):
        all_files = collected_results.get("processed", [])
    elif isinstance(collected_results, list):
        all_files = collected_results
    else:
        logging.error(f"Unexpected type for collected_results: {type(collected_results)}")
        all_files = []

    logging.info(f"Processing {len(all_files)} files for archival")

    for f in all_files:
        if not isinstance(f, dict):
            logging.warning(f"Skipping invalid file item (not a dict): {type(f)}")
            continue

        if f.get("status") != "success":
            logging.info(f"Skipping non-success file: {f}")
            continue

        original_path = f.get("original_path")

        # Handle case where original_path is a dict
        if isinstance(original_path, dict):
            original_path = original_path.get("file")

        if not original_path:
            logging.warning(f"Skipping file with missing original_path: {f}")
            continue

        try:
            # Skip if already archived
            if hook.check_for_key(original_path, bucket_name=ARCHIVE_BUCKET):
                logging.info(f"File already archived: {original_path}")
                continue

            # Copy to archive from raw bucket
            if hook.check_for_key(original_path, bucket_name=RAW_BUCKET):
                hook.copy_object(
                    source_bucket_key=original_path,
                    dest_bucket_key=f"valid/{original_path}",  # Archive under "valid" prefix
                    source_bucket_name=RAW_BUCKET,
                    dest_bucket_name=ARCHIVE_BUCKET,
                )
                hook.delete_objects(bucket=RAW_BUCKET, keys=[original_path])
                archived_count += 1
                logging.info(f"Archived: {original_path}")
            else:
                logging.warning(f"File not found in raw bucket (skipped): {original_path}")

        except Exception as e:
            logging.error(f"Failed to archive file {original_path}: {e}")

    summary = f"Archived {archived_count}/{len(all_files)} files. Merge result: {merge_result}"
    logging.info(summary)
    return summary
