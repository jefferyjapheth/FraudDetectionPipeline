"""
Schema preparation and Iceberg-ready data transformation utilities.

This module provides:
- Centralized schema definitions for all entity types.
- Schema enforcement and type coercion logic.
- SCD2 (Slowly Changing Dimension Type 2) field handling for dimension tables.
- Conversion to PyArrow tables with proper nullability metadata.

Used by transformation and upsert pipelines to ensure consistent, Iceberg-compatible
data structures.

CORRECTED SCD2 MODEL:
- dim_customers: SCD2 tracking (version, effective_date, end_date, is_current)
- dim_terminals: Static dimension (NO version tracking)
- dim_travel_profiles: SCD2 tracking (version, effective_date, end_date, is_current)
- fact_transactions: Append-only facts
"""

import logging
from typing import Dict, Optional
import pandas as pd
import pyarrow as pa

from src.config.settings import MASTER_DATA_BUCKET


# =============================================================================
#  Centralized Schema Definitions
# =============================================================================
SCHEMA_DEFINITIONS: Dict[str, Dict[str, Dict[str, str]]] = {
    "customers": {
        "required": {
            "CUSTOMER_ID": "int64",
            "HOME_CITY": "string",
            "HOME_REGION": "string",
        },
        "optional": {
            "x_customer_id": "float64",
            "y_customer_id": "float64",
            "mean_amount": "float64",
            "std_amount": "float64",
            "mean_nb_tx_per_day": "float64",
            # SCD2 fields for tracking customer changes
            "version": "int64",
            "effective_date": "datetime64[us]",
            "end_date": "datetime64[us]",
            "is_current": "bool",
            # Batch tracking
            "BATCH_ID": "string",
            "created_at": "datetime64[us]",
        },
    },
    "terminals": {
        "required": {
            "TERMINAL_ID": "int64",
            "CITY": "string",
            "REGION": "string",
        },
        "optional": {
            "x_terminal_id": "float64",
            "y_terminal_id": "float64",
            # NO SCD2 fields - terminals are static dimensions
            # Batch tracking only
            "BATCH_ID": "string",
            "created_at": "datetime64[us]",
        },
    },
    "travel_profiles": {
        "required": {
            "CUSTOMER_ID": "int64",
        },
        "optional": {
            "TRAVEL_REGIONS": "string",
            "AVG_TRAVEL_DISTANCE_KM": "float64",
            # SCD2 fields for tracking travel pattern changes
            "version": "int64",
            "effective_date": "datetime64[us]",
            "end_date": "datetime64[us]",
            "is_current": "bool",
            # Batch tracking
            "BATCH_ID": "string",
            "created_at": "datetime64[us]",
        },
    },
    "transactions": {
        "required": {
            "TRANSACTION_ID": "string",  # UUID from simulator
            "TX_DATETIME": "datetime64[us]",
            "CUSTOMER_ID": "int64",
            "TERMINAL_ID": "int64",
            "TX_AMOUNT": "float64",
        },
        "optional": {
            "TX_TIME_SECONDS": "int64",
            "TX_TIME_DAYS": "int64",
            "DISTANCE_FROM_HOME_KM": "float64",
            "TERMINAL_CITY": "string",
            "TERMINAL_REGION": "string",
            "AVG_REGION_DISTANCE_KM": "float64",
            "IS_NEW_REGION": "bool",
            "CUSTOMER_TRAVEL_REGIONS": "string",
            # Batch tracking
            "BATCH_ID": "string",
            "created_at": "datetime64[us]",
        },
    },
}


# =============================================================================
#  File Type Inference
# =============================================================================
def _infer_file_type(df: pd.DataFrame) -> Optional[str]:
    """
    Infer entity type based on DataFrame columns.

    Priority order:
      transactions > customers > terminals > travel_profiles
    """
    cols = set(df.columns)

    if {"TRANSACTION_ID", "TX_DATETIME", "CUSTOMER_ID", "TERMINAL_ID", "TX_AMOUNT"}.issubset(cols):
        return "transactions"

    if {"CUSTOMER_ID", "HOME_CITY", "HOME_REGION", "x_customer_id", "y_customer_id"}.issubset(cols):
        return "customers"

    if {"TERMINAL_ID", "CITY", "REGION", "x_terminal_id", "y_terminal_id"}.issubset(cols):
        return "terminals"

    if {"CUSTOMER_ID", "TRAVEL_REGIONS", "AVG_TRAVEL_DISTANCE_KM"}.issubset(cols):
        return "travel_profiles"

    return None


# =============================================================================
#  Schema Enforcement and Default Handling
# =============================================================================
def _default_for_dtype(dtype: str):
    """Return a default value for a given dtype string."""
    if "datetime" in dtype:
        return pd.NaT
    if dtype == "bool":
        return False
    if "float" in dtype:
        return 0.0
    if "int" in dtype:
        return 0
    if dtype == "string":
        return ""
    return None


def _enforce_schema(df: pd.DataFrame, schema: Dict[str, str]) -> pd.DataFrame:
    """Ensure all columns exist and have the correct data types."""
    for col, dtype in schema.items():
        if col not in df.columns:
            df[col] = _default_for_dtype(dtype)

        try:
            if "datetime" in dtype:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif dtype == "string":
                df[col] = df[col].astype(str)
            elif "int" in dtype:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif "float" in dtype:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dtype == "bool":
                df[col] = df[col].astype(bool)
        except Exception as e:
            logging.warning(f"Type conversion failed for column '{col}' ({dtype}): {e}")

    return df


def _fill_required_nulls(df: pd.DataFrame, schema: Dict[str, str]) -> pd.DataFrame:
    """Replace nulls in required fields with defaults."""
    for col, dtype in schema.items():
        if col in df.columns and df[col].isnull().any():
            null_count = df[col].isnull().sum()
            logging.warning(f"Filling {null_count} nulls in required column '{col}' with defaults.")
            df[col] = df[col].fillna(_default_for_dtype(dtype))
    return df


# =============================================================================
#  Main Preparation Function
# =============================================================================
def prepare_for_iceberg(df: pd.DataFrame, file_type: Optional[str] = None) -> pd.DataFrame:
    """
    Prepare a DataFrame for ingestion into an Iceberg table.

    Steps:
        1. Infer entity type if not provided.
        2. Normalize timestamps to microsecond precision.
        3. Add and populate SCD2 fields for SCD2 dimension tables (customers, travel_profiles).
        4. Ensure batch tracking fields exist (BATCH_ID, created_at).
        5. Enforce schema and fill defaults.
        6. Drop extra columns not defined in schema.
        7. Validate required fields are non-null.

    Returns:
        Iceberg-ready pandas DataFrame.
    """
    if not file_type:
        file_type = _infer_file_type(df)
        if not file_type:
            logging.warning("Could not infer file_type; returning unmodified DataFrame.")
            return df
        logging.info(f"Inferred file_type: {file_type}")

    if file_type not in SCHEMA_DEFINITIONS:
        logging.warning(f"Unknown file_type '{file_type}'; returning unmodified DataFrame.")
        return df

    schema = SCHEMA_DEFINITIONS[file_type]
    required_cols, optional_cols = schema["required"], schema["optional"]

    # Normalize datetime columns to microsecond precision (Iceberg-compatible)
    for col in df.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]", "datetime64"]).columns:
        df[col] = pd.to_datetime(df[col]).dt.tz_localize(None).dt.as_unit("us")

    # Ensure batch tracking fields exist for all entity types
    if "BATCH_ID" not in df.columns:
        df["BATCH_ID"] = "unknown_batch"
        logging.warning(f"Added missing BATCH_ID field for {file_type}")
    
    if "created_at" not in df.columns:
        df["created_at"] = pd.Timestamp.utcnow()
        logging.warning(f"Added missing created_at field for {file_type}")

    # Add SCD2 fields ONLY for SCD2 dimensions (customers, travel_profiles)
    if file_type in ["customers", "travel_profiles"]:
        logging.info(f"Adding SCD2 fields for {file_type} (SCD2 dimension)")

        if "is_current" not in df.columns:
            df["is_current"] = True
        else:
            df["is_current"] = df["is_current"].fillna(True)

        if "end_date" not in df.columns:
            df["end_date"] = pd.NaT
        else:
            df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce").dt.tz_localize(None).dt.as_unit("us")

        if "version" not in df.columns:
            df["version"] = 1
        else:
            df["version"] = df["version"].fillna(1)

        if "effective_date" not in df.columns:
            df["effective_date"] = pd.Timestamp.utcnow()
        else:
            df["effective_date"] = df["effective_date"].fillna(pd.Timestamp.utcnow())
            df["effective_date"] = pd.to_datetime(df["effective_date"]).dt.tz_localize(None).dt.as_unit("us")

        logging.info(f"SCD2 fields initialized for {file_type}")
    
    elif file_type == "terminals":
        logging.info(f"Processing terminals as static dimension (NO SCD2 fields)")
        # Explicitly remove any SCD2 fields that might have been added upstream
        scd2_fields = ["version", "effective_date", "end_date", "is_current"]
        for field in scd2_fields:
            if field in df.columns:
                df = df.drop(columns=[field])
                logging.info(f"Removed SCD2 field '{field}' from terminals (static dimension)")
    
    elif file_type == "transactions":
        logging.info(f"Processing transactions as append-only facts (NO SCD2 fields)")

    # Apply schema enforcement and fill missing values
    df = _enforce_schema(df, {**required_cols, **optional_cols})
    df = _fill_required_nulls(df, required_cols)

    # Keep only schema-defined columns
    schema_cols = list(required_cols.keys()) + list(optional_cols.keys())
    df = df[[col for col in schema_cols if col in df.columns]].copy()

    # Final required field validation
    for col, dtype in required_cols.items():
        if df[col].isnull().any():
            logging.error(f"Required column '{col}' contains nulls; filling with defaults.")
            df[col] = df[col].fillna(_default_for_dtype(dtype))

    logging.info(
        f"Prepared {file_type}: {len(df)} records, {len(df.columns)} columns "
        f"(SCD2: {file_type in ['customers', 'travel_profiles']}, Iceberg-ready)."
    )
    return df


# =============================================================================
#  Arrow Conversion
# =============================================================================
def prepare_for_iceberg_with_arrow(df: pd.DataFrame, file_type: Optional[str] = None) -> pa.Table:
    """
    Convert a DataFrame to an Iceberg-compatible PyArrow Table.

    Steps:
        1. Normalize and validate data via prepare_for_iceberg().
        2. Ensure datetime precision.
        3. Create a PyArrow table with proper nullability for required fields.

    Returns:
        PyArrow Table suitable for Iceberg ingestion.
    """
    prepared_df = prepare_for_iceberg(df, file_type)

    # Normalize datetime precision again to ensure compatibility
    for col in prepared_df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]", "datetime64"]).columns:
        prepared_df[col] = prepared_df[col].astype("datetime64[us]")

    # Convert DataFrame to Arrow Table
    arrow_table = pa.Table.from_pandas(prepared_df, preserve_index=False)

    if file_type not in SCHEMA_DEFINITIONS:
        logging.info(f"Created PyArrow table for {file_type}: {len(arrow_table)} rows.")
        return arrow_table

    schema_def = SCHEMA_DEFINITIONS[file_type]
    required_cols = schema_def["required"]

    # Adjust nullability of fields
    fields = [
        pa.field(f.name, f.type, nullable=(f.name not in required_cols))
        for f in arrow_table.schema
    ]
    new_schema = pa.schema(fields)

    arrow_table = arrow_table.cast(new_schema)
    
    # Log SCD2 status for clarity
    scd2_status = "SCD2" if file_type in ["customers", "travel_profiles"] else "static" if file_type == "terminals" else "fact"
    
    logging.info(
        f"Created PyArrow table for {file_type} ({scd2_status}) with {len(required_cols)} non-nullable fields, "
        f"{len(arrow_table)} rows total."
    )
    return arrow_table