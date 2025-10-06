def merge_transformed(files: list[dict]) -> dict:
    """
    Merge transformed file data.
    Args:
        files (list[dict]): Flat list of transformed file dicts.
    Returns:
        dict: Summary of merged data.
    """
    all_files = files or []
    total_records = sum(f.get("records_processed", 0) for f in all_files)
    return {
        "files_processed": len(all_files),
        "total_records": total_records,
        "status": "completed"
    }