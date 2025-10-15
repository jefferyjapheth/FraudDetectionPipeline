"""
S3 (MinIO) File Management Utilities
------------------------------------
This module provides generic and archival operations for moving and organizing
objects between S3-compatible buckets (MinIO). These functions are designed
for Airflow DAGs handling raw-to-archive transitions after data ingestion
or transformation tasks.
"""

import logging
from typing import List, Dict, Union
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from src.config.settings import RAW_BUCKET, ARCHIVE_BUCKET


# -----------------------------------------------------------------------------
# Generic S3 Operations
# -----------------------------------------------------------------------------
def move_file(
    hook: S3Hook,
    src_bucket: str,
    dest_bucket: str,
    key: str,
    dest_key: str = None,
    delete_source: bool = True
) -> bool:
    """
    Move or copy an object between S3 (MinIO) buckets.

    This function safely copies a file from a source bucket to a destination
    bucket, with optional cleanup of the original object. It ensures that
    existing destination objects are not overwritten by default.

    Args:
        hook: Preconfigured Airflow S3Hook instance.
        src_bucket: Source S3 bucket name.
        dest_bucket: Destination S3 bucket name.
        key: Key of the source object to move.
        dest_key: Optional destination key (defaults to same as source key).
        delete_source: Whether to delete the source object after successful copy.

    Returns:
        bool: True if the operation succeeded, False otherwise.
    """
    dest_key = dest_key or key

    try:
        # Prevent overwriting if the destination file already exists
        if hook.check_for_key(dest_key, bucket_name=dest_bucket):
            logging.info(
                f"Destination {dest_bucket}/{dest_key} already exists. Skipping copy."
            )
        else:
            # Copy object between buckets
            hook.copy_object(
                source_bucket_key=key,
                dest_bucket_key=dest_key,
                source_bucket_name=src_bucket,
                dest_bucket_name=dest_bucket,
            )
            logging.info(f"Copied {src_bucket}/{key} → {dest_bucket}/{dest_key}")

        # Optionally delete source file after copy
        if delete_source and hook.check_for_key(key, bucket_name=src_bucket):
            hook.delete_objects(bucket=src_bucket, keys=[key])
            logging.info(f"Deleted source {src_bucket}/{key}")

        return True

    except Exception as e:
        logging.error(f"Failed to move {src_bucket}/{key}: {e}", exc_info=True)
        return False


# -----------------------------------------------------------------------------
# Archive / Cleanup Utilities
# -----------------------------------------------------------------------------
def archive_processed_files(
    collected_results: Union[Dict, List],
    merge_result: Dict
) -> str:
    """
    Archive successfully processed files from the raw bucket to the archive bucket.

    This function moves files that have been marked as successfully processed
    (status='success') from the raw ingestion area to a structured archive
    path under the destination bucket. It ensures idempotency and safety by
    checking for existing archived files before moving.

    Args:
        collected_results:
            Either a dictionary containing a 'processed' key with a list of file
            metadata, or a raw list of such dictionaries. Each item is expected
            to include:
                {
                    "status": "success",
                    "original_path": "<object key>"
                }
        merge_result:
            Dictionary summarizing results from the merge/transformation step.

    Returns:
        str: A human-readable summary message of the archival process.
    """
    hook = S3Hook(aws_conn_id="minio_default")
    archived_count = 0

    # Normalize input into a list of processed file records
    if isinstance(collected_results, dict):
        all_files = collected_results.get("processed", [])
    elif isinstance(collected_results, list):
        all_files = collected_results
    else:
        logging.error(
            f"Unexpected type for collected_results: {type(collected_results)}"
        )
        all_files = []

    logging.info(
        f"Archiving {len(all_files)} processed files "
        f"from {RAW_BUCKET} to {ARCHIVE_BUCKET}"
    )

    for f in all_files:
        if not isinstance(f, dict):
            logging.warning(f"Skipping invalid file item (not a dict): {f}")
            continue

        if f.get("status") != "success":
            logging.info(f"Skipping non-success file entry: {f}")
            continue

        original_path = f.get("original_path")
        if isinstance(original_path, dict):
            original_path = original_path.get("file")

        if not original_path:
            logging.warning(f"Skipping file with missing original_path: {f}")
            continue

        try:
            dest_key = f"valid/{original_path}"

            # Skip already archived files (idempotent behavior)
            if hook.check_for_key(dest_key, bucket_name=ARCHIVE_BUCKET):
                logging.info(f"Already archived: {dest_key}")
                continue

            # Move object if it exists in the raw bucket
            if hook.check_for_key(original_path, bucket_name=RAW_BUCKET):
                hook.copy_object(
                    source_bucket_key=original_path,
                    dest_bucket_key=dest_key,
                    source_bucket_name=RAW_BUCKET,
                    dest_bucket_name=ARCHIVE_BUCKET,
                )
                hook.delete_objects(bucket=RAW_BUCKET, keys=[original_path])
                archived_count += 1
                logging.info(f"Archived: {original_path}")
            else:
                logging.warning(f"Not found in raw bucket, skipped: {original_path}")

        except Exception as e:
            logging.error(f"Failed to archive {original_path}: {e}", exc_info=True)

    summary = (
        f"Archived {archived_count}/{len(all_files)} files successfully. "
        f"Merge summary: {merge_result}"
    )
    logging.info(summary)
    return summary
