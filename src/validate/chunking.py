"""
Chunking Utilities for File Processing
--------------------------------------
This module provides helper functions for safely splitting large lists of
file metadata dictionaries into manageable chunks. The chunking logic respects
configurable size and concurrency constraints defined in Airflow settings.

Typical usage:
    - When dynamically generating Airflow tasks per file batch.
    - When batching large data ingestion jobs to stay within scheduler limits.
"""

import logging
from typing import List, Dict
from src.config.settings import CHUNK_SIZE, MAX_TOTAL_TASKS


# -----------------------------------------------------------------------------
# Chunking Helpers
# -----------------------------------------------------------------------------
def chunk_list(lst: List[Dict], n: int):
    """
    Yield successive n-sized chunks from a list.

    Args:
        lst (List[Dict]): List of items to be chunked.
        n (int): Chunk size.

    Yields:
        List[Dict]: Next chunk of items.

    Example:
        >>> list(chunk_list([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def create_chunks_safe(file_list: List[Dict]) -> List[List[Dict]]:
    """
    Split a list of file metadata dictionaries into size-limited chunks,
    respecting CHUNK_SIZE and MAX_TOTAL_TASKS.

    This function ensures that downstream Airflow task generation
    stays within operational limits and prevents DAG overload.

    Args:
        file_list (List[Dict]):
            List of file dictionaries, each containing at minimum:
            {
                "file": "<file_path>",
                "batch_id": "<batch_identifier>"
            }

    Returns:
        List[List[Dict]]:
            A list of sublists (chunks), each containing up to CHUNK_SIZE files.
            The total number of chunks will not exceed MAX_TOTAL_TASKS.

    Behavior:
        - Returns an empty list if the input list is empty or None.
        - Logs a warning if the total number of chunks exceeds MAX_TOTAL_TASKS.
    """
    if not file_list:
        logging.debug("Received empty or null file list. No chunks created.")
        return []

    # Split file list into fixed-size chunks
    chunks = list(chunk_list(file_list, CHUNK_SIZE))
    logging.info(f"Created {len(chunks)} chunks (chunk size={CHUNK_SIZE}).")

    # Enforce safety limit on total number of concurrent task batches
    if len(chunks) > MAX_TOTAL_TASKS:
        logging.warning(
            f"Chunk count ({len(chunks)}) exceeds MAX_TOTAL_TASKS={MAX_TOTAL_TASKS}. "
            f"Capping to the maximum allowed."
        )
        chunks = chunks[:MAX_TOTAL_TASKS]

    return chunks
