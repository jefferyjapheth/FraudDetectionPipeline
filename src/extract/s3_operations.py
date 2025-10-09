import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def move_file(hook: S3Hook, src_bucket, dest_bucket, key, dest_key=None, delete_source=True):
    dest_key = dest_key or key
    try:
        # Check if destination already exists
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

        # Delete source if it exists
        if delete_source and hook.check_for_key(key, bucket_name=src_bucket):
            hook.delete_objects(bucket=src_bucket, keys=[key])
            logging.info(f"Deleted source {src_bucket}/{key}")

        return True
    except Exception as e:
        logging.error(f" Failed to move {key}: {e}")
        return False
