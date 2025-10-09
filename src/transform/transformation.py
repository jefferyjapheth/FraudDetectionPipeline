import logging
import pandas as pd
from typing import Dict, List
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import StringIO

def transform_and_join(file_batches: Dict[str, List[Dict]], s3_hook: S3Hook, bucket_name: str) -> Dict[str, pd.DataFrame]:
    """
    Transform and join raw CSVs from all types into master datasets.
    Handles SCD2 for dimension tables and prepares fact transactions.

    Args:
        file_batches (Dict[str, List[Dict]]): Dict with keys
            ["customers", "terminals", "transactions", "travel_profiles"],
            each containing list of file metadata dicts with 'file', 'batch_id'.
        s3_hook (S3Hook): Airflow S3 hook for reading CSVs from S3.
        bucket_name (str): S3 bucket where files are stored.

    Returns:
        Dict[str, pd.DataFrame]: Prepared DataFrames ready for Iceberg upsert
            with keys: customers, terminals, travel_profiles, transactions
    """
    dfs = {}

    for file_type, files in file_batches.items():
        all_data = []

        for f in files or []:
            file_key = f["file"]
            batch_id = f.get("batch_id")
            try:
                # Read CSV from S3
                obj = s3_hook.get_key(file_key, bucket_name=bucket_name)
                csv_data = obj.get()["Body"].read().decode("utf-8")
                df = pd.read_csv(StringIO(csv_data))

                # Add metadata columns
                df["BATCH_ID"] = batch_id
                df["created_at"] = pd.Timestamp.utcnow()
                all_data.append(df)
            except Exception as e:
                logging.error(f"Failed to read {file_key} from S3: {e}")

        if all_data:
            df_combined = pd.concat(all_data, ignore_index=True)
            logging.info(f"{file_type}: Combined {len(df_combined)} records from {len(all_data)} files")
            dfs[file_type] = df_combined
        else:
            dfs[file_type] = pd.DataFrame()  # empty DF if no data

    # -----------------------------
    # Handle SCD2 for dimensions
    # -----------------------------
    for dim in ["customers", "terminals", "travel_profiles"]:
        df = dfs.get(dim)
        if df.empty:
            continue

        if dim in ["customers", "terminals"]:
            df["is_current"] = True
            df["end_date"] = pd.NaT
            df["effective_date"] = pd.to_datetime(df["effective_date"], utc=True)
            df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
        elif dim == "travel_profiles":
            df["created_at"] = pd.to_datetime(df["created_at"], utc=True)

        dfs[dim] = df

    # -----------------------------
    # Join dimensions to transactions
    # -----------------------------
    if not dfs.get("transactions").empty:
        df_tx = dfs["transactions"]

        if not dfs.get("customers").empty:
            df_tx = df_tx.merge(
                dfs["customers"][["CUSTOMER_ID", "HOME_CITY", "HOME_REGION"]],
                on="CUSTOMER_ID",
                how="left"
            )

        if not dfs.get("terminals").empty:
            df_tx = df_tx.merge(
                dfs["terminals"][["TERMINAL_ID", "CITY", "REGION"]],
                left_on="TERMINAL_ID",
                right_on="TERMINAL_ID",
                how="left",
                suffixes=("", "_terminal")
            )

        if not dfs.get("travel_profiles").empty:
            df_tx = df_tx.merge(
                dfs["travel_profiles"][["CUSTOMER_ID", "TRAVEL_REGIONS", "AVG_TRAVEL_DISTANCE_KM"]],
                on="CUSTOMER_ID",
                how="left"
            )

        dfs["transactions"] = df_tx

    return dfs
