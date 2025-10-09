"""
Minimal Iceberg Upsert Operations
"""
import logging
import pandas as pd
from datetime import datetime
from io import StringIO
from typing import List, Dict
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, LongType, DoubleType, 
    TimestampType, BooleanType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, BucketTransform
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from src.config.settings import ICEBERG_CATALOG_CONFIG, ICEBERG_NAMESPACE, RAW_BUCKET


def get_catalog():
    """Initialize Iceberg catalog."""
    return load_catalog("minio_catalog", **ICEBERG_CATALOG_CONFIG)


def ensure_namespace():
    """Create namespace if not exists."""
    catalog = get_catalog()
    try:
        catalog.create_namespace(ICEBERG_NAMESPACE)
        logging.info(f"✅ Created namespace: {ICEBERG_NAMESPACE}")
    except Exception:
        logging.info(f"Namespace {ICEBERG_NAMESPACE} already exists")


def create_customers_table():
    """Create dim_customers table with SCD2 support."""
    catalog = get_catalog()
    table_name = f"{ICEBERG_NAMESPACE}.dim_customers"
    
    try:
        catalog.load_table(table_name)
        logging.info(f"Table {table_name} already exists")
        return
    except NoSuchTableError:
        pass
    
    schema = Schema(
        NestedField(1, "CUSTOMER_ID", LongType(), required=True),
        NestedField(2, "HOME_CITY", StringType(), required=True),
        NestedField(3, "HOME_REGION", StringType(), required=True),
        NestedField(4, "x_customer_id", DoubleType(), required=True),
        NestedField(5, "y_customer_id", DoubleType(), required=True),
        NestedField(6, "mean_amount", DoubleType(), required=True),
        NestedField(7, "std_amount", DoubleType(), required=True),
        NestedField(8, "mean_nb_tx_per_day", DoubleType(), required=True),
        NestedField(9, "version", LongType(), required=True),
        NestedField(10, "effective_date", TimestampType(), required=True),
        NestedField(11, "end_date", TimestampType(), required=False),
        NestedField(12, "is_current", BooleanType(), required=True),
        NestedField(13, "BATCH_ID", StringType(), required=False),
        NestedField(14, "created_at", TimestampType(), required=False),
    )
    
    partition_spec = PartitionSpec(
        PartitionField(source_id=10, field_id=1000, transform=DayTransform(), name="effective_date_day")
    )
    
    catalog.create_table(table_name, schema=schema, partition_spec=partition_spec)
    logging.info(f"✅ Created table: {table_name}")


def create_terminals_table():
    """Create dim_terminals table with SCD2 support."""
    catalog = get_catalog()
    table_name = f"{ICEBERG_NAMESPACE}.dim_terminals"
    
    try:
        catalog.load_table(table_name)
        logging.info(f"Table {table_name} already exists")
        return
    except NoSuchTableError:
        pass
    
    schema = Schema(
        NestedField(1, "TERMINAL_ID", LongType(), required=True),
        NestedField(2, "CITY", StringType(), required=True),
        NestedField(3, "REGION", StringType(), required=True),
        NestedField(4, "x_terminal_id", DoubleType(), required=True),
        NestedField(5, "y_terminal_id", DoubleType(), required=True),
        NestedField(6, "version", LongType(), required=True),
        NestedField(7, "effective_date", TimestampType(), required=True),
        NestedField(8, "end_date", TimestampType(), required=False),
        NestedField(9, "is_current", BooleanType(), required=True),
        NestedField(10, "BATCH_ID", StringType(), required=False),
        NestedField(11, "created_at", TimestampType(), required=False),
    )
    
    partition_spec = PartitionSpec(
        PartitionField(source_id=7, field_id=1000, transform=DayTransform(), name="effective_date_day")
    )
    
    catalog.create_table(table_name, schema=schema, partition_spec=partition_spec)
    logging.info(f"✅ Created table: {table_name}")


def create_transactions_table():
    """Create fact_transactions table (append-only)."""
    catalog = get_catalog()
    table_name = f"{ICEBERG_NAMESPACE}.fact_transactions"
    
    try:
        catalog.load_table(table_name)
        logging.info(f"Table {table_name} already exists")
        return
    except NoSuchTableError:
        pass
    
    schema = Schema(
        NestedField(1, "TRANSACTION_ID", StringType(), required=True),
        NestedField(2, "TX_DATETIME", TimestampType(), required=True),
        NestedField(3, "CUSTOMER_ID", LongType(), required=True),
        NestedField(4, "TERMINAL_ID", LongType(), required=True),
        NestedField(5, "TX_AMOUNT", DoubleType(), required=True),
        NestedField(6, "TX_TIME_SECONDS", LongType(), required=True),
        NestedField(7, "TX_TIME_DAYS", LongType(), required=True),
        NestedField(8, "DISTANCE_FROM_HOME_KM", DoubleType(), required=True),
        NestedField(9, "TERMINAL_CITY", StringType(), required=True),
        NestedField(10, "TERMINAL_REGION", StringType(), required=True),
        NestedField(11, "AVG_REGION_DISTANCE_KM", DoubleType(), required=False),
        NestedField(12, "IS_NEW_REGION", LongType(), required=True),
        NestedField(13, "CUSTOMER_TRAVEL_REGIONS", StringType(), required=False),
        NestedField(14, "BATCH_ID", StringType(), required=False),
        NestedField(15, "ingested_at", TimestampType(), required=False),
    )
    
    partition_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="tx_date")
    )
    
    catalog.create_table(table_name, schema=schema, partition_spec=partition_spec)
    logging.info(f"✅ Created table: {table_name}")


def create_travel_profiles_table():
    """Create dim_travel_profiles table."""
    catalog = get_catalog()
    table_name = f"{ICEBERG_NAMESPACE}.dim_travel_profiles"
    
    try:
        catalog.load_table(table_name)
        logging.info(f"Table {table_name} already exists")
        return
    except NoSuchTableError:
        pass
    
    schema = Schema(
        NestedField(1, "CUSTOMER_ID", LongType(), required=True),
        NestedField(2, "TRAVEL_REGIONS", StringType(), required=False),
        NestedField(3, "AVG_TRAVEL_DISTANCE_KM", DoubleType(), required=False),
        NestedField(4, "BATCH_ID", StringType(), required=False),
        NestedField(5, "created_at", TimestampType(), required=False),
    )
    
    partition_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=BucketTransform(16), name="customer_bucket")
    )
    
    catalog.create_table(table_name, schema=schema, partition_spec=partition_spec)
    logging.info(f"✅ Created table: {table_name}")


def upsert_to_iceberg(file_info: Dict = None, s3_hook: S3Hook = None, df: pd.DataFrame = None) -> Dict:
    """
    Upsert a dataset into Iceberg tables. Accepts either a DataFrame or S3 file reference.

    Args:
        file_info: Dict with 'file', 'type', 'batch_id' (required if df is None)
        s3_hook: S3Hook instance (required if df is None)
        df: Optional pandas DataFrame (already loaded and transformed)

    Returns:
        Dict with operation summary
    """
    if df is None:
        # fallback to S3 read if df not provided
        if not file_info or not s3_hook:
            raise ValueError("file_info and s3_hook are required if df is None")
        
        file_key = file_info["file"]
        file_type = file_info["type"]
        batch_id = file_info.get("batch_id")
        
        obj = s3_hook.get_key(file_key, bucket_name=RAW_BUCKET)
        csv_data = obj.get()["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(csv_data))
    else:
        file_type = file_info.get("type") if file_info else "unknown"
        batch_id = file_info.get("batch_id") if file_info else None

    # Add metadata
    if "BATCH_ID" not in df:
        df["BATCH_ID"] = batch_id
    if "created_at" not in df:
        df["created_at"] = pd.Timestamp.utcnow()

    catalog = get_catalog()

    if file_type == "customers":
        df["is_current"] = True
        df["end_date"] = pd.NaT
        df["effective_date"] = pd.to_datetime(df["effective_date"], utc=True)
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
        df["CUSTOMER_ID"] = df["CUSTOMER_ID"].astype("int64")

        table = catalog.load_table(f"{ICEBERG_NAMESPACE}.dim_customers")
        table.append(df)
        logging.info(f"✅ Upserted {len(df)} customers")
        return {"type": "customers", "records": len(df)}

    elif file_type == "terminals":
        df["is_current"] = True
        df["end_date"] = pd.NaT
        df["effective_date"] = pd.to_datetime(df["effective_date"], utc=True)
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
        df["TERMINAL_ID"] = df["TERMINAL_ID"].astype("int64")

        table = catalog.load_table(f"{ICEBERG_NAMESPACE}.dim_terminals")
        table.append(df)
        logging.info(f"✅ Upserted {len(df)} terminals")
        return {"type": "terminals", "records": len(df)}

    elif file_type == "transactions":
        df["TX_DATETIME"] = pd.to_datetime(df["TX_DATETIME"], utc=True)
        df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)

        table = catalog.load_table(f"{ICEBERG_NAMESPACE}.fact_transactions")
        table.append(df)
        logging.info(f"✅ Appended {len(df)} transactions")
        return {"type": "transactions", "records": len(df)}

    elif file_type == "travel_profiles":
        df["CUSTOMER_ID"] = df["CUSTOMER_ID"].astype("int64")
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True)

        table = catalog.load_table(f"{ICEBERG_NAMESPACE}.dim_travel_profiles")
        table.append(df)
        logging.info(f"✅ Upserted {len(df)} travel profiles")
        return {"type": "travel_profiles", "records": len(df)}

    logging.warning(f"No matching Iceberg table for type {file_type}")
    return {"type": file_type, "records": 0}


def initialize_iceberg_tables():
    """Initialize all Iceberg tables."""
    ensure_namespace()
    create_customers_table()
    create_terminals_table()
    create_transactions_table()
    create_travel_profiles_table()
    logging.info("✅ All Iceberg tables initialized")