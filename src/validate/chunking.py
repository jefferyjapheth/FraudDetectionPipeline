import logging
from src.config.settings import CHUNK_SIZE, MAX_TOTAL_TASKS
from typing import List, Dict

def chunk_list(lst: List[Dict], n: int):
    """Yield successive n-sized chunks from the list of file dicts."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def create_chunks_safe(file_list: List[Dict]) -> List[List[Dict]]:
    """
    Split a list of file dicts (with 'file' and 'batch_id') into chunks safely,
    respecting CHUNK_SIZE and MAX_TOTAL_TASKS.

    Args:
        file_list (List[Dict]): List of file dicts with keys 'file' and 'batch_id'.

    Returns:
        List[List[Dict]]: Chunked list of file dicts.
    """
    if not file_list:
        return []

    chunks = list(chunk_list(file_list, CHUNK_SIZE))

    if len(chunks) > MAX_TOTAL_TASKS:
        chunks = chunks[:MAX_TOTAL_TASKS]
        logging.warning(f"Too many chunks, capped at {MAX_TOTAL_TASKS}")

    return chunks
