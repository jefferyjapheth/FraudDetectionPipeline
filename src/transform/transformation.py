import logging
import pandas as pd
from typing import Dict, Optional
from src.config.settings import MASTER_DATA_BUCKET


# ──────────────────────────────────────────────
#  Centralized schema definitions
# ──────────────────────────────────────────────
SCHEMA_DEFINITIONS: Dict[str, Dict[str, Dict[str, str]]] = {
    "customers": {
        "required": {
            "CUSTOMER_ID": "int64",
            "HOME_CITY": "string",
            "HOME_REGION": "string",
            "x_customer_id": "float64",
            "y_customer_id": "float64",
            "mean_amount": "float64",
            "std_amount": "float64",
            "mean_nb_tx_per_day": "float64",
            "version": "int64",
            "effective_date": "datetime64[us]",
            "is_current": "bool",
        },
        "optional": {
            "end_date": "datetime64[us]",
            "BATCH_ID": "string",
            "created_at": "datetime64[us]",
        },
    },
    "terminals": {
        "required": {
            "TERMINAL_ID": "int64",
            "CITY": "string",
            "REGION": "string",
            "x_terminal_id": "float64",
            "y_terminal_id": "float64",
            "version": "int64",
            "effective_date": "datetime64[us]",
            "is_current": "bool",
        },
        "optional": {
            "end_date": "datetime64[us]",
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
            "BATCH_ID": "string",
            "created_at": "datetime64[us]",
        },
    },
    "transactions": {
        "required": {
            "TRANSACTION_ID": "int64",
            "TX_DATETIME": "datetime64[us]",
            "CUSTOMER_ID": "int64",
            "TERMINAL_ID": "int64",
            "TX_AMOUNT": "float64",
        },
        "optional": {
            "TX_TIME_SECONDS": "float64",
            "TX_TIME_DAYS": "float64",
            "DISTANCE_FROM_HOME_KM": "float64",
            "TERMINAL_CITY": "string",
            "TERMINAL_REGION": "string",
            "AVG_REGION_DISTANCE_KM": "float64",
            "IS_NEW_REGION": "bool",
            "CUSTOMER_TRAVEL_REGIONS": "string",
            "BATCH_ID": "string",
            "ingested_at": "datetime64[us]",
        },
    },
}


# ──────────────────────────────────────────────
#  Helper: infer file_type based on columns
# ──────────────────────────────────────────────
def _infer_file_type(df: pd.DataFrame) -> Optional[str]:
    cols = set(df.columns)
    if {"CUSTOMER_ID", "HOME_CITY", "HOME_REGION"}.issubset(cols):
        return "customers"
    if {"TERMINAL_ID", "CITY", "REGION"}.issubset(cols):
        return "terminals"
    if {"TRANSACTION_ID", "TX_DATETIME", "CUSTOMER_ID", "TERMINAL_ID"}.issubset(cols):
        return "transactions"
    if {"TRAVEL_REGIONS", "AVG_TRAVEL_DISTANCE_KM"}.issubset(cols):
        return "travel_profiles"
    return None


# ──────────────────────────────────────────────
#  Helper: default values and schema enforcement
# ──────────────────────────────────────────────
def _default_for_dtype(dtype: str):
    if "datetime" in dtype:
        return pd.NaT
    if dtype == "bool":
        return False
    if "float" in dtype or "int" in dtype:
        return 0
    return ""


def _enforce_schema(df: pd.DataFrame, schema: Dict[str, str]) -> pd.DataFrame:
    for col, dtype in schema.items():
        if col not in df.columns:
            df[col] = _default_for_dtype(dtype)
        df[col] = df[col].astype(dtype, errors="ignore")
    return df


def _fill_required_nulls(df: pd.DataFrame, schema: Dict[str, str]):
    for col, dtype in schema.items():
        if df[col].isnull().any():
            logging.warning(f"Filling nulls for required column '{col}' with defaults.")
            df[col] = df[col].fillna(_default_for_dtype(dtype))
    return df


# ──────────────────────────────────────────────
#  Main Function: Prepare for Iceberg
# ──────────────────────────────────────────────
def prepare_for_iceberg(df: pd.DataFrame, file_type: Optional[str] = None) -> pd.DataFrame:
    """
    Prepare DataFrame for Iceberg ingestion:
    - Infer file type if not provided
    - Normalize timestamps to microsecond precision
    - Enforce schema and fill missing defaults
    - Output Iceberg-ready DataFrame
    """

    # Infer file type if not provided
    if not file_type:
        file_type = _infer_file_type(df)
        if not file_type:
            logging.warning("⚠️ Could not infer file_type; returning unmodified DataFrame.")
            return df
        logging.info(f"🧩 Inferred file_type: '{file_type}'")

    if file_type not in SCHEMA_DEFINITIONS:
        logging.warning(f"⚠️ Unknown file_type '{file_type}'. Returning unmodified DataFrame.")
        return df

    schema = SCHEMA_DEFINITIONS[file_type]
    required_cols, optional_cols = schema["required"], schema["optional"]

    # Normalize timestamps
    for col in df.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]", "datetime64"]).columns:
        df[col] = pd.to_datetime(df[col]).dt.tz_localize(None).dt.as_unit("us")

    # Apply schema
    df = _enforce_schema(df, {**required_cols, **optional_cols})
    df = _fill_required_nulls(df, required_cols)

    # Final column ordering
    schema_cols = list(required_cols.keys()) + list(optional_cols.keys())
    df = df[[col for col in schema_cols if col in df.columns]].copy()

    logging.info(f"✅ Prepared {file_type}: {len(df)} records, {len(df.columns)} cols (Iceberg-ready)")
    return df
