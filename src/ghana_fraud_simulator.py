"""
ghana_fraud_simulator_scd2.py

Enhanced Ghana Fraud Simulator with SCD Type 2–ready seeds and auto-archiving.
Uses real geographic coordinates (x=longitude, y=latitude) while preserving
existing column names for downstream compatibility.

Produces:
 - customers, terminals, travel_profiles CSVs
 - transactions in batched CSVs
 - metadata JSON with batch manifest
Uploads optionally to S3/MinIO and archives previous seeds.
"""

import os
import uuid
import math
import random
import json
import shutil
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Tuple, Dict

import pandas as pd
import numpy as np
import boto3
from botocore.exceptions import ClientError
from math import radians, sin, cos, sqrt, atan2

# Configure structured logging for production usage
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("ghana_fraud_simulator")

# =============================================================================
# Ghana Regions and Cities (real coords: x = longitude, y = latitude)
# =============================================================================
# Note: we intentionally keep the column names x/y to remain backward-compatible
GHANA_CITIES = [
    {"city": "Accra", "region": "Greater Accra", "x": -0.1870, "y": 5.6037},
    {"city": "Kumasi", "region": "Ashanti", "x": -1.6244, "y": 6.6885},
    {"city": "Tamale", "region": "Northern", "x": -0.8420, "y": 9.4034},
    {"city": "Takoradi", "region": "Western", "x": -1.7554, "y": 4.8980},
    {"city": "Cape Coast", "region": "Central", "x": -1.2466, "y": 5.1053},
    {"city": "Ho", "region": "Volta", "x": 0.4713, "y": 6.6018},
]

REGIONS = list({c["region"] for c in GHANA_CITIES})


# =============================================================================
# Geographic Utilities
# =============================================================================
def haversine_distance(x1: float, y1: float, x2: float, y2: float) -> float:
    """
    Compute great-circle distance (in kilometers) between two points.
    Input: x = longitude, y = latitude (decimal degrees).
    """
    # Earth radius in kilometers
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(radians, (y1, x1, y2, x2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return round(R * c, 2)


def compute_average_region_distances() -> Dict[Tuple[str, str], float]:
    """
    Compute average pairwise inter-region distances using city coordinates.
    Returns a mapping (region_a, region_b) -> average_distance_km.
    """
    region_centroids: Dict[str, Tuple[float, float]] = {}
    for r in REGIONS:
        cities = [c for c in GHANA_CITIES if c["region"] == r]
        # centroid in (lon, lat) stored as (x_mean, y_mean)
        region_centroids[r] = (
            float(np.mean([c["x"] for c in cities])),
            float(np.mean([c["y"] for c in cities])),
        )

    avg_distances: Dict[Tuple[str, str], float] = {}
    for r1 in REGIONS:
        for r2 in REGIONS:
            x1, y1 = region_centroids[r1]
            x2, y2 = region_centroids[r2]
            avg_distances[(r1, r2)] = haversine_distance(x1, y1, x2, y2)
    return avg_distances


# =============================================================================
# Seed Archiving / Evolution (SCD2-ready)
# =============================================================================
def archive_old_seed(seed_path: Path) -> None:
    """
    Archive an existing seed file to a dated folder before overwriting.
    Example: data/seeds/archive/20251015/customers_seed_120501.csv
    """
    if seed_path.exists():
        archive_dir = seed_path.parent / "archive" / datetime.utcnow().strftime("%Y%m%d")
        archive_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%H%M%S")
        archived_name = f"{seed_path.stem}_{timestamp}.csv"
        archived_path = archive_dir / archived_name
        shutil.move(seed_path, archived_path)
        logger.info("Archived old seed to %s", archived_path)


def evolve_customers(customers: pd.DataFrame, evolve_prob: float = 0.1) -> pd.DataFrame:
    """
    Randomly evolve a subset of customers (simulate SCD Type 2).
    Updates:
      - version increment
      - effective_date to now (ISO)
      - HOME_CITY/HOME_REGION and x/y coordinates (small noise)
      - mean_amount/std_amount recalculated
    """
    updated = []
    for _, row in customers.iterrows():
        if random.random() < evolve_prob:
            city_info = random.choice(GHANA_CITIES)
            new_row = row.copy()
            new_row["version"] = int(new_row.get("version", 1)) + 1
            new_row["effective_date"] = datetime.utcnow().isoformat()
            new_row["HOME_CITY"] = city_info["city"]
            new_row["HOME_REGION"] = city_info["region"]
            # small noise in degrees (~ up to a few km)
            new_row["x_customer_id"] = city_info["x"] + float(np.random.normal(0, 0.02))
            new_row["y_customer_id"] = city_info["y"] + float(np.random.normal(0, 0.02))
            new_row["mean_amount"] = float(np.random.uniform(10, 200))
            new_row["std_amount"] = float(new_row["mean_amount"] / 2)
            updated.append(new_row)
    if updated:
        customers = pd.concat([customers, pd.DataFrame(updated)], ignore_index=True)
    return customers


def evolve_terminals(terminals: pd.DataFrame, evolve_prob: float = 0.05) -> pd.DataFrame:
    """
    Randomly evolve terminals (simulate relocations/updates).
    Updates:
      - version increment
      - effective_date to now (ISO)
      - CITY/REGION and x/y coordinates (small noise)
    """
    updated = []
    for _, row in terminals.iterrows():
        if random.random() < evolve_prob:
            city_info = random.choice(GHANA_CITIES)
            new_row = row.copy()
            new_row["version"] = int(new_row.get("version", 1)) + 1
            new_row["effective_date"] = datetime.utcnow().isoformat()
            new_row["CITY"] = city_info["city"]
            new_row["REGION"] = city_info["region"]
            new_row["x_terminal_id"] = city_info["x"] + float(np.random.normal(0, 0.02))
            new_row["y_terminal_id"] = city_info["y"] + float(np.random.normal(0, 0.02))
            updated.append(new_row)
    if updated:
        terminals = pd.concat([terminals, pd.DataFrame(updated)], ignore_index=True)
    return terminals


def load_or_generate_customers(n_customers: int, seed_dir: str = "data/seeds", reset: bool = False) -> pd.DataFrame:
    """Load existing customers seed or generate new customers if reset or missing."""
    os.makedirs(seed_dir, exist_ok=True)
    seed_path = Path(seed_dir) / "customers_seed.csv"

    if not reset and seed_path.exists():
        customers = pd.read_csv(seed_path)
        customers = evolve_customers(customers)
        logger.info("Loaded and evolved customers seed: %s rows", len(customers))
    else:
        if seed_path.exists():
            archive_old_seed(seed_path)
        customers = generate_customers(n_customers)
        logger.info("Generated new customers seed: %s rows", len(customers))

    customers.to_csv(seed_path, index=False)
    return customers


def load_or_generate_terminals(n_terminals: int, seed_dir: str = "data/seeds", reset: bool = False) -> pd.DataFrame:
    """Load existing terminals seed or generate new terminals if reset or missing."""
    os.makedirs(seed_dir, exist_ok=True)
    seed_path = Path(seed_dir) / "terminals_seed.csv"

    if not reset and seed_path.exists():
        terminals = pd.read_csv(seed_path)
        terminals = evolve_terminals(terminals)
        logger.info("Loaded and evolved terminals seed: %s rows", len(terminals))
    else:
        if seed_path.exists():
            archive_old_seed(seed_path)
        terminals = generate_terminals(n_terminals)
        logger.info("Generated new terminals seed: %s rows", len(terminals))

    terminals.to_csv(seed_path, index=False)
    return terminals


# =============================================================================
# Generators
# =============================================================================
def generate_customers(n_customers: int = 500, seed: int = 0) -> pd.DataFrame:
    """Generate customers with real-location coordinates (x=lon, y=lat)."""
    np.random.seed(seed)
    now = datetime.utcnow().isoformat()
    customers = []

    for i in range(n_customers):
        city_info = random.choice(GHANA_CITIES)
        mean_amount = float(np.random.uniform(10, 200))
        std_amount = float(mean_amount / 2.0)
        mean_tx_day = float(np.random.uniform(1, 5))

        customers.append({
            "CUSTOMER_ID": int(i),
            "HOME_CITY": city_info["city"],
            "HOME_REGION": city_info["region"],
            # x = longitude, y = latitude; add small noise
            "x_customer_id": float(city_info["x"] + np.random.normal(0, 0.02)),
            "y_customer_id": float(city_info["y"] + np.random.normal(0, 0.02)),
            "mean_amount": mean_amount,
            "std_amount": std_amount,
            "mean_nb_tx_per_day": mean_tx_day,
            "version": 1,
            "effective_date": now,
        })

    df = pd.DataFrame(customers)
    logger.debug("Generated customers dataframe with columns: %s", df.columns.tolist())
    return df


def generate_terminals(n_terminals: int = 1000, seed: int = 1) -> pd.DataFrame:
    """Generate terminals with real-location coordinates (x=lon, y=lat)."""
    np.random.seed(seed)
    now = datetime.utcnow().isoformat()
    terminals = []

    for i in range(n_terminals):
        city_info = random.choice(GHANA_CITIES)
        terminals.append({
            "TERMINAL_ID": int(i),
            "CITY": city_info["city"],
            "REGION": city_info["region"],
            "x_terminal_id": float(city_info["x"] + np.random.normal(0, 0.02)),
            "y_terminal_id": float(city_info["y"] + np.random.normal(0, 0.02)),
            "version": 1,
            "effective_date": now,
        })

    df = pd.DataFrame(terminals)
    logger.debug("Generated terminals dataframe with columns: %s", df.columns.tolist())
    return df


def generate_customer_travel_profiles(customers: pd.DataFrame, avg_region_distances: Dict[Tuple[str, str], float], max_regions: int = 3) -> pd.DataFrame:
    """Create travel profiles (comma-separated regions and avg distance)."""
    profiles = []
    for _, cust in customers.iterrows():
        travel_regions = random.sample(REGIONS, k=random.randint(1, max_regions))
        avg_dist = float(np.mean([avg_region_distances[(cust["HOME_REGION"], r)] for r in travel_regions]))
        profiles.append({
            "CUSTOMER_ID": int(cust["CUSTOMER_ID"]),
            "TRAVEL_REGIONS": ",".join(travel_regions),
            "AVG_TRAVEL_DISTANCE_KM": round(avg_dist, 2),
        })
    return pd.DataFrame(profiles)


def generate_transactions(
    customers: pd.DataFrame,
    terminals: pd.DataFrame,
    avg_region_distances: Dict[Tuple[str, str], float],
    travel_profiles: pd.DataFrame,
    window_minutes: int = 60,
    p_high_amount: float = 0.05
) -> pd.DataFrame:
    """
    Generate transaction events.
    DISTANCE_FROM_HOME_KM is computed by haversine_distance using x/lon & y/lat.
    """
    tx_list = []
    now = pd.Timestamp.now()
    window_seconds = window_minutes * 60

    for _, cust in customers.iterrows():
        # number of transactions is Poisson-distributed around mean_nb_tx_per_day
        nb_tx = np.random.poisson(max(0.1, cust.get("mean_nb_tx_per_day", 1)))
        profile = travel_profiles.loc[travel_profiles["CUSTOMER_ID"] == cust["CUSTOMER_ID"]].iloc[0]
        allowed_regions = profile["TRAVEL_REGIONS"].split(",")

        for _ in range(nb_tx):
            tx_dt = now - pd.Timedelta(seconds=int(np.random.uniform(0, window_seconds)))
            terminal = terminals.sample(1).iloc[0]
            amount = max(0.1, float(np.random.normal(cust["mean_amount"], cust["std_amount"])))
            if random.random() < p_high_amount:
                amount *= float(np.random.uniform(3, 8))

            dist_km = haversine_distance(
                cust["x_customer_id"], cust["y_customer_id"],
                terminal["x_terminal_id"], terminal["y_terminal_id"]
            )
            is_new_region = terminal["REGION"] not in allowed_regions

            tx_list.append({
                "TRANSACTION_ID": str(uuid.uuid4()),
                "TX_DATETIME": tx_dt,
                "CUSTOMER_ID": int(cust["CUSTOMER_ID"]),
                "TERMINAL_ID": int(terminal["TERMINAL_ID"]),
                "TX_AMOUNT": round(amount, 2),
                "TX_TIME_SECONDS": int((tx_dt - (now - pd.Timedelta(seconds=window_seconds))).total_seconds()),
                "TX_TIME_DAYS": 0,
                "DISTANCE_FROM_HOME_KM": round(dist_km, 2),
                "TERMINAL_CITY": terminal["CITY"],
                "TERMINAL_REGION": terminal["REGION"],
                "AVG_REGION_DISTANCE_KM": avg_region_distances[(cust["HOME_REGION"], terminal["REGION"])],
                "IS_NEW_REGION": bool(is_new_region),
                "CUSTOMER_TRAVEL_REGIONS": ",".join(allowed_regions),
            })

    df = pd.DataFrame(tx_list)
    logger.info("Generated transactions: %s rows", len(df))
    return df


# =============================================================================
# S3 / File Persistence
# =============================================================================
def init_s3_client():
    """Initialize a boto3 S3 client using environment variables or defaults."""
    config = {
        "endpoint": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        "access_key": os.getenv("MINIO_USER", "admin"),
        "secret_key": os.getenv("MINIO_PASSWORD", "password"),
        "bucket": os.getenv("MINIO_BUCKET_RAW", "raw-data"),
    }

    s3_client = boto3.client(
        "s3",
        endpoint_url=config["endpoint"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )
    return s3_client, config["bucket"]


def ensure_bucket(s3_client, bucket_name: str) -> None:
    """Ensure that the specified S3 bucket exists, creating it if necessary."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        err_code = e.response.get("Error", {}).get("Code", "")
        if err_code in ["404", "NoSuchBucket"]:
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info("Created bucket: %s", bucket_name)
        else:
            logger.exception("Error while ensuring bucket %s: %s", bucket_name, e)
            raise


def record_metadata(uploaded_files: list, batch_id: str, output_dir: str = "data/raw_batches", upload_to_s3: bool = False) -> str:
    """Write metadata JSON and optionally upload to S3."""
    os.makedirs(output_dir, exist_ok=True)
    metadata = {
        "batch_id": batch_id,
        "generated_at": datetime.utcnow().isoformat(),
        "files": uploaded_files,
    }

    metadata_filename = f"metadata_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    metadata_path = os.path.join(output_dir, metadata_filename)

    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    shutil.copy(metadata_path, os.path.join(output_dir, "metadata_latest.json"))
    logger.info("Metadata written to %s", metadata_path)

    if upload_to_s3:
        s3_client, bucket = init_s3_client()
        ensure_bucket(s3_client, bucket)
        s3_client.upload_file(metadata_path, bucket, f"metadata/{os.path.basename(metadata_path)}")
        s3_client.upload_file(os.path.join(output_dir, "metadata_latest.json"), bucket, "metadata/metadata_latest.json")
        logger.info("Uploaded metadata to s3://%s/metadata/", bucket)

    return metadata_path


def save_entity(df: pd.DataFrame, name: str, prefix: str, out_dir: str = "data/raw_batches", upload_to_s3: bool = False, uploaded_files: list = None) -> None:
    """Persist a single dataset locally and optionally upload to S3."""
    os.makedirs(os.path.join(out_dir, prefix), exist_ok=True)
    filename = f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    path = os.path.join(out_dir, prefix, filename)
    df.to_csv(path, index=False)
    logger.info("Saved %s rows to %s", len(df), path)

    s3_path = None
    if upload_to_s3:
        s3_client, bucket = init_s3_client()
        ensure_bucket(s3_client, bucket)
        s3_client.upload_file(path, bucket, f"{prefix}/{filename}")
        s3_path = f"s3://{bucket}/{prefix}/{filename}"
        logger.info("Uploaded %s to %s", path, s3_path)

    if uploaded_files is not None:
        uploaded_files.append({"type": prefix, "local_path": path, "s3_path": s3_path or "N/A", "rows": len(df)})


def save_batches(df: pd.DataFrame, batch_size: int = 5000, prefix: str = "transactions", out_dir: str = "data/raw_batches", upload_to_s3: bool = False, uploaded_files: list = None, batch_id: str = None) -> None:
    """Persist transactions in batches and optionally upload to S3."""
    os.makedirs(os.path.join(out_dir, prefix), exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    s3_client, bucket = (init_s3_client() if upload_to_s3 else (None, None))
    if upload_to_s3 and bucket:
        ensure_bucket(s3_client, bucket)

    for i, start in enumerate(range(0, len(df), batch_size), start=1):
        batch = df.iloc[start:start + batch_size]
        filename = f"{prefix}_{timestamp}_batch_{i:03d}.csv"
        path = os.path.join(out_dir, prefix, filename)
        batch.to_csv(path, index=False)
        logger.info("Wrote batch %s (%s rows) to %s", i, len(batch), path)

        s3_path = None
        if upload_to_s3 and bucket:
            s3_client.upload_file(path, bucket, f"{prefix}/{filename}")
            s3_path = f"s3://{bucket}/{prefix}/{filename}"
            logger.info("Uploaded batch %s to s3://%s/%s", i, bucket, filename)

        if uploaded_files is not None:
            uploaded_files.append({"type": prefix, "local_path": path, "s3_path": s3_path or "N/A", "rows": len(batch), "batch_id": batch_id, "batch_num": i})


# =============================================================================
# Main
# =============================================================================
def main(reset_seed: bool = False) -> None:
    """Main entry point for the fraud simulator."""
    n_customers = random.randint(10, 60)
    n_terminals = random.randint(5, 50)
    window_minutes = 15
    batch_size = 20
    out_dir = "data/raw_batches"
    upload_to_s3 = True

    batch_id = str(uuid.uuid4())
    logger.info("Starting Ghana Fraud Simulator | Batch ID: %s", batch_id)
    logger.info("Generating %s customers and %s terminals", n_customers, n_terminals)

    avg_region_distances = compute_average_region_distances()
    customers = load_or_generate_customers(n_customers, reset=reset_seed)
    terminals = load_or_generate_terminals(n_terminals, reset=reset_seed)
    travel_profiles = generate_customer_travel_profiles(customers, avg_region_distances)

    transactions = generate_transactions(customers, terminals, avg_region_distances, travel_profiles, window_minutes=window_minutes)

    uploaded_files = []
    save_entity(customers, "customers", "customers", out_dir, upload_to_s3, uploaded_files)
    save_entity(terminals, "terminals", "terminals", out_dir, upload_to_s3, uploaded_files)
    save_entity(travel_profiles, "travel_profiles", "travel_profiles", out_dir, upload_to_s3, uploaded_files)
    save_batches(transactions, batch_size=batch_size, prefix="transactions", out_dir=out_dir, upload_to_s3=upload_to_s3, uploaded_files=uploaded_files, batch_id=batch_id)

    record_metadata(uploaded_files, batch_id, output_dir=out_dir, upload_to_s3=upload_to_s3)
    logger.info("Simulation complete | Batch ID: %s", batch_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ghana Fraud Data Simulator (SCD Type 2 Ready)")
    parser.add_argument("--reset-seed", action="store_true", help="Reset customer/terminal seed data")
    args = parser.parse_args()
    main(reset_seed=args.reset_seed)
