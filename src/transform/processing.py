import logging
from typing import List, Dict
from src.config.settings import STAGING_BUCKET

def process_file_batch(file_batch: List[Dict]) -> List[Dict]:
    if not file_batch:
        logging.info("No files to process in this batch.")
        return []

    processed = []
    for file_dict in file_batch:
        if not isinstance(file_dict, dict):
            logging.warning(f"Skipping invalid item in batch: {file_dict}")
            continue

        file_path = file_dict.get("file")
        batch_id = file_dict.get("batch_id")

        if not file_path:
            logging.warning(f"Skipping item with missing 'file': {file_dict}")
            continue

        processed_path = f"processed_{file_path}"

        try:
            logging.info(f"Processing file: {STAGING_BUCKET}/{file_path} (batch: {batch_id})")

            processed.append({
                "original_path": {"file": file_path, "batch_id": batch_id},
                "processed_path": processed_path,
                "status": "success"
            })
        except Exception as e:
            logging.error(f"Failed to process file {file_path} (batch: {batch_id}): {e}")
            processed.append({
                "original_path": {"file": file_path, "batch_id": batch_id},
                "processed_path": None,
                "status": "failed",
                "error": str(e)
            })

    logging.info(f"Processed {len(processed)} files in this batch.")
    return processed
