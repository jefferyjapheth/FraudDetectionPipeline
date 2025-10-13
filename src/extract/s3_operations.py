import logging
from typing import List, Dict, Union
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from src.config.settings import RAW_BUCKET, ARCHIVE_BUCKET

# --------------------------------------------------------------------------------
# Generic S3 Operations
# --------------------------------------------------------------------------------

def move_file(
    hook: S3Hook,
    src_bucket: str,
    dest_bucket: str,
    key: str,
    dest_key: str = None,
    delete_source: bool = True
) -> bool:
    """
    Move or copy a file between S3 (MinIO) buckets.

    Args:
        hook: S3Hook instance
        src_bucket: Source bucket name
        dest_bucket: Destination bucket name
        key: Object key to move
        dest_key: Optional new key name (defaults to same as source)
        delete_source: Whether to delete source object after copy

    Returns:
        bool: True if success, False otherwise
    """
    dest_key = dest_key or key
    try:
        if hook.check_for_key(dest_key, bucket_name=dest_bucket):
            logging.info(f"Destination {dest_bucket}/{dest_key} already exists, skipping copy")
        else:
            hook.copy_object(
                source_bucket_key=key,
                dest_bucket_key=dest_key,
                source_bucket_name=src_bucket,
                dest_bucket_name=dest_bucket,
            )
            logging.info(f"Copied {src_bucket}/{key} → {dest_bucket}/{dest_key}")

        if delete_source and hook.check_for_key(key, bucket_name=src_bucket):
            hook.delete_objects(bucket=src_bucket, keys=[key])
            logging.info(f"Deleted source {src_bucket}/{key}")

        return True

    except Exception as e:
        logging.error(f"❌ Failed to move {key}: {e}")
        return False


# --------------------------------------------------------------------------------
# Archive / Cleanup Utilities
# --------------------------------------------------------------------------------

def archive_processed_files(collected_results: Union[Dict, List], merge_result: Dict) -> str:
    """
    Archive successfully processed files from the raw bucket to the archive bucket.

    Args:
        collected_results: Either a dict with 'processed' key or a list of processed file dicts
        merge_result: Summary dict from the merge/transformation step

    Returns:
        str: Summary message
    """
    hook = S3Hook(aws_conn_id="minio_default")
    archived_count = 0

    if isinstance(collected_results, dict):
        all_files = collected_results.get("processed", [])
    elif isinstance(collected_results, list):
        all_files = collected_results
    else:
        logging.error(f"Unexpected type for collected_results: {type(collected_results)}")
        all_files = []

    logging.info(f"Archiving {len(all_files)} processed files from {RAW_BUCKET} to {ARCHIVE_BUCKET}")

    for f in all_files:
        if not isinstance(f, dict):
            logging.warning(f"Skipping invalid file item (not a dict): {f}")
            continue

        if f.get("status") != "success":
            logging.info(f"Skipping non-success file: {f}")
            continue

        original_path = f.get("original_path")
        if isinstance(original_path, dict):
            original_path = original_path.get("file")

        if not original_path:
            logging.warning(f"Skipping file with missing original_path: {f}")
            continue

        try:
            dest_key = f"valid/{original_path}"

            if hook.check_for_key(dest_key, bucket_name=ARCHIVE_BUCKET):
                logging.info(f"Already archived: {dest_key}")
                continue

            if hook.check_for_key(original_path, bucket_name=RAW_BUCKET):
                hook.copy_object(
                    source_bucket_key=original_path,
                    dest_bucket_key=dest_key,
                    source_bucket_name=RAW_BUCKET,
                    dest_bucket_name=ARCHIVE_BUCKET,
                )
                hook.delete_objects(bucket=RAW_BUCKET, keys=[original_path])
                archived_count += 1
                logging.info(f"✅ Archived: {original_path}")
            else:
                logging.warning(f"Not found in raw bucket (skipped): {original_path}")

        except Exception as e:
            logging.error(f"❌ Failed to archive {original_path}: {e}")

    summary = f"Archived {archived_count}/{len(all_files)} files. Merge summary: {merge_result}"
    logging.info(summary)
    return summary
