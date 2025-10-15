"""
SCD2 Handler - Manages Slowly Changing Dimension Type 2 Logic
Tracks historical changes by comparing incoming data with existing Iceberg records

CRITICAL FIX: Proper index handling for boolean Series filtering
"""

import logging
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from datetime import datetime
from src.config.settings import ICEBERG_CATALOG_CONFIG, ICEBERG_NAMESPACE


def get_catalog():
    """Initialize Iceberg catalog."""
    return load_catalog("minio_catalog", **ICEBERG_CATALOG_CONFIG)


def apply_scd2_logic(
    incoming_df: pd.DataFrame,
    file_type: str,
    business_keys: list,
    compare_columns: list
) -> pd.DataFrame:
    """
    Apply SCD2 logic by comparing incoming data with existing Iceberg records.
    
    Process:
    1. Load existing "current" records from Iceberg (is_current = True)
    2. For each incoming record:
       - If NEW: Insert with is_current=True, version=1
       - If EXISTS but UNCHANGED: Skip (no re-insert)
       - If EXISTS and CHANGED:
         a. Close old version (is_current=False, end_date=now)
         b. Insert new version (is_current=True, version+1, effective_date=now)
    
    Args:
        incoming_df: New data to upsert
        file_type: Type of dimension (customers, terminals)
        business_keys: Columns that uniquely identify a record (e.g., ["CUSTOMER_ID"])
        compare_columns: Columns to check for changes (e.g., ["HOME_CITY", "HOME_REGION"])
    
    Returns:
        DataFrame ready for append with proper SCD2 fields set
    """
    
    table_map = {
        "customers": "dim_customers",
        "terminals": "dim_terminals"
    }
    
    if file_type not in table_map:
        logging.warning(f"SCD2 not applicable for {file_type}, returning as-is")
        return incoming_df
    
    table_name = f"{ICEBERG_NAMESPACE}.{table_map[file_type]}"
    
    # Use timezone-naive timestamp for Iceberg compatibility
    now = pd.Timestamp.utcnow().tz_localize(None)
    
    try:
        # Load existing records from Iceberg
        catalog = get_catalog()
        table = catalog.load_table(table_name)
        
        # Read ALL records to get version history
        all_existing_df = table.scan().to_pandas()
        
        # Normalize datetime columns from Iceberg (remove timezone if present)
        datetime_cols = all_existing_df.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64']).columns
        for col in datetime_cols:
            if col in all_existing_df.columns:
                all_existing_df[col] = pd.to_datetime(all_existing_df[col]).dt.tz_localize(None)
        
        if all_existing_df.empty:
            logging.info(f"No existing records in {table_name}, treating all as new")
            incoming_df["is_current"] = True
            incoming_df["end_date"] = pd.NaT
            if "version" not in incoming_df.columns:
                incoming_df["version"] = 1
            if "effective_date" not in incoming_df.columns:
                incoming_df["effective_date"] = now
            return incoming_df
        
        # Filter to only current records for comparison
        existing_current_df = all_existing_df[all_existing_df["is_current"] == True].copy()
        
        # CRITICAL: Reset index to ensure alignment in boolean filtering
        existing_current_df = existing_current_df.reset_index(drop=True)
        
        logging.info(f"Found {len(all_existing_df)} total records ({len(existing_current_df)} current) in {table_name}")
        
    except NoSuchTableError:
        logging.info(f"Table {table_name} doesn't exist yet, treating all as new")
        incoming_df["is_current"] = True
        incoming_df["end_date"] = pd.NaT
        if "version" not in incoming_df.columns:
            incoming_df["version"] = 1
        if "effective_date" not in incoming_df.columns:
            incoming_df["effective_date"] = now
        return incoming_df
    except Exception as e:
        logging.error(f"Error reading existing records: {e}")
        # Fallback: treat all as new
        incoming_df["is_current"] = True
        incoming_df["end_date"] = pd.NaT
        if "version" not in incoming_df.columns:
            incoming_df["version"] = 1
        if "effective_date" not in incoming_df.columns:
            incoming_df["effective_date"] = now
        return incoming_df
    
    # Process incoming records and compare with existing
    records_to_append = []
    
    new_count = 0
    changed_count = 0
    unchanged_count = 0
    
    for _, incoming_row in incoming_df.iterrows():
        # CRITICAL FIX: Build mask properly with reset index
        # Initialize mask with all True values for the existing DataFrame
        mask = pd.Series([True] * len(existing_current_df), index=existing_current_df.index)
        
        # Build compound boolean mask for business key matching
        for key in business_keys:
            if key in existing_current_df.columns and key in incoming_row.index:
                # Use .values to avoid index alignment issues
                key_matches = (existing_current_df[key].values == incoming_row[key])
                mask = mask & pd.Series(key_matches, index=existing_current_df.index)
        
        # Apply mask to find matching records
        matching_records = existing_current_df[mask]
        
        if matching_records.empty:
            # NEW RECORD - insert as current
            new_record = incoming_row.copy()
            new_record["is_current"] = True
            new_record["end_date"] = pd.NaT
            new_record["version"] = 1
            new_record["effective_date"] = now
            records_to_append.append(new_record)
            new_count += 1
            logging.debug(f"New record: {business_keys[0]}={incoming_row[business_keys[0]]}")
            
        else:
            # EXISTING RECORD - check for changes
            existing_record = matching_records.iloc[0]
            
            # Compare specified columns for changes
            has_changed = False
            changed_fields = []
            for col in compare_columns:
                if col in incoming_row.index and col in existing_record.index:
                    incoming_val = incoming_row[col]
                    existing_val = existing_record[col]
                    
                    # Handle NaN comparison
                    if pd.isna(incoming_val) and pd.isna(existing_val):
                        continue
                    
                    if incoming_val != existing_val:
                        has_changed = True
                        changed_fields.append(col)
            
            if has_changed:
                # RECORD CHANGED - close old version and insert new
                changed_count += 1
                logging.info(f"Change detected for {business_keys[0]}={incoming_row[business_keys[0]]}: {changed_fields}")
                
                # 1. Close existing record (set is_current=False, end_date=now)
                closed_record = existing_record.copy()
                closed_record["is_current"] = False
                closed_record["end_date"] = now
                records_to_append.append(closed_record)
                
                # 2. Insert new version
                new_version = incoming_row.copy()
                new_version["is_current"] = True
                new_version["end_date"] = pd.NaT
                new_version["version"] = int(existing_record["version"]) + 1
                new_version["effective_date"] = now
                records_to_append.append(new_version)
                
            else:
                # NO CHANGE - skip (don't re-insert)
                unchanged_count += 1
                logging.debug(f"No change for {business_keys[0]}={incoming_row[business_keys[0]]}, skipping")
    
    logging.info(f"SCD2 Summary: {new_count} new, {changed_count} changed, {unchanged_count} unchanged")
    
    if not records_to_append:
        logging.warning("No records to append after SCD2 processing (all unchanged)")
        return pd.DataFrame()
    
    result_df = pd.DataFrame(records_to_append)
    
    # Ensure all datetime columns are timezone-naive
    datetime_cols = result_df.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64']).columns
    for col in datetime_cols:
        if col in result_df.columns:
            result_df[col] = pd.to_datetime(result_df[col]).dt.tz_localize(None)
    
    logging.info(f"SCD2 processing complete: {len(result_df)} records to append")
    return result_df


def get_scd2_config(file_type: str) -> dict:
    """
    Get SCD2 configuration for each dimension type.
    
    Returns:
        dict: {
            "business_keys": [...],      # Columns that uniquely identify a record
            "compare_columns": [...]     # Columns to check for changes
        }
    """
    configs = {
        "customers": {
            "business_keys": ["CUSTOMER_ID"],
            "compare_columns": [
                "HOME_CITY",
                "HOME_REGION",
                "x_customer_id",
                "y_customer_id",
                "mean_amount",
                "std_amount",
                "mean_nb_tx_per_day"
            ]
        },
        "terminals": {
            "business_keys": ["TERMINAL_ID"],
            "compare_columns": [
                "CITY",
                "REGION",
                "x_terminal_id",
                "y_terminal_id"
            ]
        }
    }
    
    return configs.get(file_type, {"business_keys": [], "compare_columns": []})