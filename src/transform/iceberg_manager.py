import logging
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
    BooleanType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, BucketTransform
from src.config.settings import ICEBERG_CATALOG_CONFIG, ICEBERG_NAMESPACE


# =====================================================================
#  Iceberg Catalog Utilities
# =====================================================================

def get_catalog():
    """
    Initialize and return the Iceberg catalog.
    
    Uses configuration from ICEBERG_CATALOG_CONFIG defined in project settings.
    """
    return load_catalog("minio_catalog", **ICEBERG_CATALOG_CONFIG)


def ensure_namespace():
    """
    Ensure that the target Iceberg namespace exists.
    
    If the namespace already exists, the operation is logged and skipped.
    """
    catalog = get_catalog()
    try:
        catalog.create_namespace(ICEBERG_NAMESPACE)
        logging.info(f"Created namespace: {ICEBERG_NAMESPACE}")
    except Exception:
        logging.info(f"Namespace {ICEBERG_NAMESPACE} already exists.")


# =====================================================================
#  Table Definition and Initialization
# =====================================================================

def create_table_if_missing(name: str, schema: Schema, partition_spec: PartitionSpec):
    """
    Create an Iceberg table if it does not already exist.
    
    Args:
        name: Table name within the namespace.
        schema: PyIceberg Schema object defining the table structure.
        partition_spec: Partition specification for the table.
    """
    catalog = get_catalog()
    table_name = f"{ICEBERG_NAMESPACE}.{name}"

    try:
        catalog.load_table(table_name)
        logging.info(f"Table {table_name} already exists.")
    except NoSuchTableError:
        catalog.create_table(table_name, schema=schema, partition_spec=partition_spec)
        logging.info(f"Created table: {table_name}")


def initialize_iceberg_tables():
    """
    Initialize all Iceberg tables required by the pipeline.

    This includes dimension and fact tables for customers, terminals,
    transactions, and travel profiles.
    """
    ensure_namespace()

    # ------------------ Dimension: Customers ------------------
    create_table_if_missing(
        "dim_customers",
        Schema(
            NestedField(1, "CUSTOMER_ID", LongType(), required=True),
            NestedField(2, "HOME_CITY", StringType(), required=True),
            NestedField(3, "HOME_REGION", StringType(), required=True),
            NestedField(4, "x_customer_id", DoubleType()),
            NestedField(5, "y_customer_id", DoubleType()),
            NestedField(6, "mean_amount", DoubleType()),
            NestedField(7, "std_amount", DoubleType()),
            NestedField(8, "mean_nb_tx_per_day", DoubleType()),
            NestedField(9, "version", LongType()),
            NestedField(10, "effective_date", TimestampType()),
            NestedField(11, "end_date", TimestampType()),
            NestedField(12, "is_current", BooleanType()),
            NestedField(13, "BATCH_ID", StringType()),
            NestedField(14, "created_at", TimestampType()),
        ),
        PartitionSpec(PartitionField(10, 1000, DayTransform(), "effective_date_day")),
    )

    # ------------------ Dimension: Terminals ------------------
    create_table_if_missing(
        "dim_terminals",
        Schema(
            NestedField(1, "TERMINAL_ID", LongType(), required=True),
            NestedField(2, "CITY", StringType(), required=True),
            NestedField(3, "REGION", StringType(), required=True),
            NestedField(4, "x_terminal_id", DoubleType()),
            NestedField(5, "y_terminal_id", DoubleType()),
            NestedField(6, "version", LongType()),
            NestedField(7, "effective_date", TimestampType()),
            NestedField(8, "end_date", TimestampType()),
            NestedField(9, "is_current", BooleanType()),
            NestedField(10, "BATCH_ID", StringType()),
            NestedField(11, "created_at", TimestampType()),
        ),
        PartitionSpec(PartitionField(7, 1000, DayTransform(), "effective_date_day")),
    )

    # ------------------ Fact: Transactions ------------------
    create_table_if_missing(
        "fact_transactions",
        Schema(
            NestedField(1, "TRANSACTION_ID", StringType(), required=True),
            NestedField(2, "TX_DATETIME", TimestampType(), required=True),
            NestedField(3, "CUSTOMER_ID", LongType(), required=True),
            NestedField(4, "TERMINAL_ID", LongType(), required=True),
            NestedField(5, "TX_AMOUNT", DoubleType(), required=True),
            NestedField(6, "TX_TIME_SECONDS", LongType()),
            NestedField(7, "TX_TIME_DAYS", LongType()),
            NestedField(8, "DISTANCE_FROM_HOME_KM", DoubleType()),
            NestedField(9, "TERMINAL_CITY", StringType()),
            NestedField(10, "TERMINAL_REGION", StringType()),
            NestedField(11, "AVG_REGION_DISTANCE_KM", DoubleType()),
            NestedField(12, "IS_NEW_REGION", BooleanType()),
            NestedField(13, "CUSTOMER_TRAVEL_REGIONS", StringType()),
            NestedField(14, "BATCH_ID", StringType()),
            NestedField(15, "created_at", TimestampType()),
        ),
        PartitionSpec(PartitionField(2, 1000, DayTransform(), "tx_date")),
    )

    # ------------------ Dimension: Travel Profiles ------------------
    create_table_if_missing(
        "dim_travel_profiles",
        Schema(
            NestedField(1, "CUSTOMER_ID", LongType(), required=True),
            NestedField(2, "TRAVEL_REGIONS", StringType()),
            NestedField(3, "AVG_TRAVEL_DISTANCE_KM", DoubleType()),
            NestedField(4, "BATCH_ID", StringType()),
            NestedField(5, "created_at", TimestampType()),
        ),
        PartitionSpec(PartitionField(1, 1000, BucketTransform(16), "customer_bucket")),
    )

    logging.info("All Iceberg tables successfully initialized.")


# =====================================================================
#  Data Upsert Operations
# =====================================================================

def upsert_to_iceberg(file_type: str, arrow_table: pa.Table):
    """
    Append a PyArrow table to its corresponding Iceberg target.
    
    Args:
        file_type: Logical type of the dataset (e.g., 'customers', 'transactions').
        arrow_table: PyArrow Table to be appended.
    
    Returns:
        dict: Summary of the upsert operation.
    """
    table_map = {
        "customers": "dim_customers",
        "terminals": "dim_terminals",
        "transactions": "fact_transactions",
        "travel_profiles": "dim_travel_profiles",
    }

    catalog = get_catalog()
    table_name = table_map.get(file_type)

    if not table_name:
        logging.error(f"No Iceberg table mapping found for '{file_type}'.")
        return {"type": file_type, "records": 0, "status": "failed"}

    table = catalog.load_table(f"{ICEBERG_NAMESPACE}.{table_name}")
    table.append(arrow_table)

    logging.info(
        f"Upserted {arrow_table.num_rows} {file_type} records into {table_name}"
    )
    return {"type": file_type, "records": arrow_table.num_rows, "status": "success"}


def upsert_dataframe_to_iceberg(file_type: str, df: pd.DataFrame):
    """
    Convert a DataFrame to an Arrow Table and upsert it to Iceberg.

    Args:
        file_type: Dataset type (e.g., 'customers', 'transactions').
        df: Pandas DataFrame to be written.

    Returns:
        dict: Summary of the upsert operation.
    """
    if df.empty:
        logging.warning(f"Empty DataFrame for {file_type}, skipping upsert.")
        return {"type": file_type, "records": 0, "status": "empty"}

    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
    return upsert_to_iceberg(file_type, arrow_table)


def upsert_to_iceberg_with_scd2(file_type: str, arrow_table: pa.Table):
    """
    Perform upsert to Iceberg, applying Slowly Changing Dimension (Type 2) logic
    for dimension tables (customers, terminals).

    For fact tables, a simple append is performed.

    Args:
        file_type: Data type identifier ('customers', 'terminals', 'transactions', etc.).
        arrow_table: PyArrow Table containing the new records.

    Returns:
        dict: Summary of the upsert operation.
    """
    table_map = {
        "customers": "dim_customers",
        "terminals": "dim_terminals",
        "transactions": "fact_transactions",
        "travel_profiles": "dim_travel_profiles",
    }

    catalog = get_catalog()
    table_name = table_map.get(file_type)

    if not table_name:
        logging.error(f"No Iceberg table mapping found for '{file_type}'.")
        return {"type": file_type, "records": 0, "status": "failed"}

    table = catalog.load_table(f"{ICEBERG_NAMESPACE}.{table_name}")

    # Apply SCD2 logic only to dimension tables
    if file_type in ["customers", "terminals"]:
        from src.transform.scd2_handler import apply_scd2_logic, get_scd2_config
        from src.transform.transformation import prepare_for_iceberg_with_arrow

        # Convert Arrow Table to DataFrame for processing
        df = arrow_table.to_pandas()

        # Retrieve SCD2 configuration (business keys and comparison columns)
        scd2_config = get_scd2_config(file_type)

        # Apply SCD2 merge logic
        df_scd2 = apply_scd2_logic(
            df,
            file_type,
            business_keys=scd2_config["business_keys"],
            compare_columns=scd2_config["compare_columns"],
        )

        # Skip if there are no new or changed records
        if df_scd2.empty:
            logging.info(f"No changes detected for {file_type}, skipping upsert.")
            return {"type": file_type, "records": 0, "status": "no_changes"}

        # Convert processed DataFrame back to Arrow format
        arrow_table = prepare_for_iceberg_with_arrow(df_scd2, file_type=file_type)

    # Append final Arrow Table to Iceberg
    table.append(arrow_table)

    logging.info(
        f"Upserted {arrow_table.num_rows} {file_type} records into {table_name}"
    )
    return {"type": file_type, "records": arrow_table.num_rows, "status": "success"}
