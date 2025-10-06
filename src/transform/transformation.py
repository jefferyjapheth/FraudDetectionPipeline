import logging
from typing import List, Dict

def transform_data(processed_files: List[Dict]) -> List[Dict]:
    """
    Transform processed files into transformed metadata, including batch_id.

    Args:
        processed_files (List[Dict]): List of dicts from process_file_batch.

    Returns:
        List[Dict]: List of transformed file metadata.
    """
    transformed = []
    for f in processed_files or []:
        if not isinstance(f, dict):
            logging.warning(f"Skipping invalid item in processed_files: {f}")
            continue

        if f.get("status") != "success":
            logging.info(f"Skipping failed file: {f.get('original_path')}")
            continue

        transformed.append({
            "original_path": f.get("original_path"),
            "transformed_path": f"transformed_{f.get('processed_path')}",
            "records_processed": 1000,  # TODO: Replace with actual logic
            "batch_id": f.get("batch_id")
        })

    logging.info(f"Transformed {len(transformed)} files")
    return transformed
