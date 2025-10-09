"""
ghana_fraud_simulator_scd2.py
Enhanced Ghana Fraud Simulator with SCD Type 2–ready seeds and auto-archiving.
Simulates customer/terminal evolution over time and uploads raw data to S3.
"""

import os
import uuid
import math
import random
import json
import shutil
import pandas as pd
import numpy as np
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from pathlib import Path
import argparse

# -----------------------------
# Ghana Regions & Cities
# -----------------------------
GHANA_CITIES = [
    {"city": "Accra", "region": "Greater Accra", "x": 0, "y": 0},
    {"city": "Kumasi", "region": "Ashanti", "x": 400, "y": 100},
    {"city": "Tamale", "region": "Northern", "x": 500, "y": 400},
    {"city": "Takoradi", "region": "Western", "x": -200, "y": -100},
    {"city": "Cape Coast", "region": "Central", "x": -100, "y": -50},
    {"city": "Ho", "region": "Volta", "x": 50, "y": 200},
]
REGIONS = list({c["region"] for c in GHANA_CITIES})


# -----------------------------
# Distance Utilities
# -----------------------------
def haversine_distance(x1, y1, x2, y2):
    dx, dy = x2 - x1, y2 - y1
    return math.sqrt(dx * dx + dy * dy)


def compute_average_region_distances():
    region_coords = {}
    for r in REGIONS:
        cities = [c for c in GHANA_CITIES if c["region"] == r]
        region_coords[r] = (np.mean([c["x"] for c in cities]), np.mean([c["y"] for c in cities]))
    avg_distances = {}
    for r1 in REGIONS:
        for r2 in REGIONS:
            x1, y1 = region_coords[r1]
            x2, y2 = region_coords[r2]
            avg_distances[(r1, r2)] = round(haversine_distance(x1, y1, x2, y2), 2)
    return avg_distances


# -----------------------------
# Seed Archiving Utility
# -----------------------------
def archive_old_seed(seed_path: Path):
    """Archive existing seed file to a dated folder before overwriting."""
    if seed_path.exists():
        archive_dir = seed_path.parent / "archive" / datetime.utcnow().strftime("%Y%m%d")
        archive_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%H%M%S")
        archived_name = f"{seed_path.stem}_{timestamp}.csv"
        archived_path = archive_dir / archived_name
        shutil.move(seed_path, archived_path)
        print(f"🗄️ Archived old seed to {archived_path}")


# -----------------------------
# Seed Persistence & Evolution
# -----------------------------
def evolve_customers(customers, evolve_prob=0.1):
    """Randomly evolve a small subset of customers (simulate SCD2 changes)."""
    updated = []
    for _, row in customers.iterrows():
        if random.random() < evolve_prob:
            city_info = random.choice(GHANA_CITIES)
            new_row = row.copy()
            new_row["version"] += 1
            new_row["effective_date"] = datetime.utcnow().isoformat()
            new_row["HOME_CITY"] = city_info["city"]
            new_row["HOME_REGION"] = city_info["region"]
            new_row["x_customer_id"] = city_info["x"] + np.random.normal(0, 5)
            new_row["y_customer_id"] = city_info["y"] + np.random.normal(0, 5)
            new_row["mean_amount"] = np.random.uniform(10, 200)
            new_row["std_amount"] = new_row["mean_amount"] / 2
            updated.append(new_row)
    if updated:
        customers = pd.concat([customers, pd.DataFrame(updated)], ignore_index=True)
    return customers


def evolve_terminals(terminals, evolve_prob=0.05):
    """Randomly evolve some terminals (simulate relocations)."""
    updated = []
    for _, row in terminals.iterrows():
        if random.random() < evolve_prob:
            city_info = random.choice(GHANA_CITIES)
            new_row = row.copy()
            new_row["version"] += 1
            new_row["effective_date"] = datetime.utcnow().isoformat()
            new_row["CITY"] = city_info["city"]
            new_row["REGION"] = city_info["region"]
            new_row["x_terminal_id"] = city_info["x"] + np.random.normal(0, 5)
            new_row["y_terminal_id"] = city_info["y"] + np.random.normal(0, 5)
            updated.append(new_row)
    if updated:
        terminals = pd.concat([terminals, pd.DataFrame(updated)], ignore_index=True)
    return terminals


def load_or_generate_customers(n_customers, seed_dir="data/seeds", reset=False):
    os.makedirs(seed_dir, exist_ok=True)
    seed_path = Path(seed_dir) / "customers_seed.csv"
    if not reset and seed_path.exists():
        customers = pd.read_csv(seed_path)
        customers = evolve_customers(customers)
    else:
        if seed_path.exists():
            archive_old_seed(seed_path)
        customers = generate_customers(n_customers)
    customers.to_csv(seed_path, index=False)
    return customers


def load_or_generate_terminals(n_terminals, seed_dir="data/seeds", reset=False):
    os.makedirs(seed_dir, exist_ok=True)
    seed_path = Path(seed_dir) / "terminals_seed.csv"
    if not reset and seed_path.exists():
        terminals = pd.read_csv(seed_path)
        terminals = evolve_terminals(terminals)
    else:
        if seed_path.exists():
            archive_old_seed(seed_path)
        terminals = generate_terminals(n_terminals)
    terminals.to_csv(seed_path, index=False)
    return terminals


# -----------------------------
# Generators
# -----------------------------
def generate_customers(n_customers=500, seed=0):
    np.random.seed(seed)
    customers = []
    now = datetime.utcnow().isoformat()
    for i in range(n_customers):
        city_info = random.choice(GHANA_CITIES)
        mean_amount = np.random.uniform(10, 200)
        std_amount = mean_amount / 2
        mean_tx_day = np.random.uniform(1, 5)
        customers.append({
            "CUSTOMER_ID": i,
            "HOME_CITY": city_info["city"],
            "HOME_REGION": city_info["region"],
            "x_customer_id": city_info["x"] + np.random.normal(0, 5),
            "y_customer_id": city_info["y"] + np.random.normal(0, 5),
            "mean_amount": mean_amount,
            "std_amount": std_amount,
            "mean_nb_tx_per_day": mean_tx_day,
            "version": 1,
            "effective_date": now
        })
    return pd.DataFrame(customers)


def generate_terminals(n_terminals=1000, seed=1):
    np.random.seed(seed)
    terminals = []
    now = datetime.utcnow().isoformat()
    for i in range(n_terminals):
        city_info = random.choice(GHANA_CITIES)
        terminals.append({
            "TERMINAL_ID": i,
            "CITY": city_info["city"],
            "REGION": city_info["region"],
            "x_terminal_id": city_info["x"] + np.random.normal(0, 5),
            "y_terminal_id": city_info["y"] + np.random.normal(0, 5),
            "version": 1,
            "effective_date": now
        })
    return pd.DataFrame(terminals)


def generate_customer_travel_profiles(customers, avg_region_distances, max_regions=3):
    profiles = []
    for _, cust in customers.iterrows():
        travel_regions = random.sample(REGIONS, k=random.randint(1, max_regions))
        avg_dist = np.mean([avg_region_distances[(cust["HOME_REGION"], r)] for r in travel_regions])
        profiles.append({
            "CUSTOMER_ID": cust["CUSTOMER_ID"],
            "TRAVEL_REGIONS": travel_regions,
            "AVG_TRAVEL_DISTANCE_KM": round(avg_dist, 2)
        })
    return pd.DataFrame(profiles)


def generate_transactions(customers, terminals, avg_region_distances, travel_profiles,
                          window_minutes=60, p_high_amount=0.05):
    tx_list = []
    now = pd.Timestamp.now()
    window_seconds = window_minutes * 60

    for _, cust in customers.iterrows():
        nb_tx = np.random.poisson(cust["mean_nb_tx_per_day"])
        profile = travel_profiles[travel_profiles["CUSTOMER_ID"] == cust["CUSTOMER_ID"]].iloc[0]
        allowed_regions = profile["TRAVEL_REGIONS"]

        for _ in range(nb_tx):
            tx_dt = now - pd.Timedelta(seconds=int(np.random.uniform(0, window_seconds)))
            terminal = terminals.sample(1).iloc[0]
            amount = max(0.1, np.random.normal(cust["mean_amount"], cust["std_amount"]))
            if random.random() < p_high_amount:
                amount *= np.random.uniform(3, 8)
            dist_km = round(haversine_distance(
                cust["x_customer_id"], cust["y_customer_id"],
                terminal["x_terminal_id"], terminal["y_terminal_id"]
            ), 2)
            is_new_region = 0 if terminal["REGION"] in allowed_regions else 1

            tx_list.append({
                "TRANSACTION_ID": str(uuid.uuid4()),
                "TX_DATETIME": tx_dt,
                "CUSTOMER_ID": cust["CUSTOMER_ID"],
                "TERMINAL_ID": terminal["TERMINAL_ID"],
                "TX_AMOUNT": round(amount, 2),
                "TX_TIME_SECONDS": int((tx_dt - (now - pd.Timedelta(seconds=window_seconds))).total_seconds()),
                "TX_TIME_DAYS": 0,
                "DISTANCE_FROM_HOME_KM": dist_km,
                "TERMINAL_CITY": terminal["CITY"],
                "TERMINAL_REGION": terminal["REGION"],
                "AVG_REGION_DISTANCE_KM": avg_region_distances[(cust["HOME_REGION"], terminal["REGION"])],
                "IS_NEW_REGION": is_new_region,
                "CUSTOMER_TRAVEL_REGIONS": ",".join(allowed_regions)
            })
    return pd.DataFrame(tx_list)


# -----------------------------
# S3 / Save / Metadata
# -----------------------------
def init_s3_client():
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
        region_name="us-east-1"
    )
    return s3_client, config["bucket"]


def ensure_bucket(s3_client, bucket_name: str):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response["Error"]["Code"] in ["404", "NoSuchBucket"]:
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            raise e


def record_metadata(uploaded_files, batch_id, output_dir="data/raw_batches", upload_to_s3=False):
    os.makedirs(output_dir, exist_ok=True)
    metadata = {
        "batch_id": batch_id,
        "generated_at": datetime.utcnow().isoformat(),
        "files": uploaded_files
    }
    metadata_filename = f"metadata_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    metadata_path = os.path.join(output_dir, metadata_filename)
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    shutil.copy(metadata_path, os.path.join(output_dir, "metadata_latest.json"))
    print(f" Metadata written to {metadata_path}")

    if upload_to_s3:
        s3_client, bucket = init_s3_client()
        ensure_bucket(s3_client, bucket)
        s3_client.upload_file(metadata_path, bucket, f"metadata/{os.path.basename(metadata_path)}")
        s3_client.upload_file(os.path.join(output_dir, "metadata_latest.json"), bucket, "metadata/metadata_latest.json")
        print(f"Uploaded metadata files to s3://{bucket}/metadata/")
    return metadata_path


def save_entity(df, name, prefix, out_dir="data/raw_batches", upload_to_s3=False, uploaded_files=None):
    os.makedirs(os.path.join(out_dir, prefix), exist_ok=True)
    filename = f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    path = os.path.join(out_dir, prefix, filename)
    df.to_csv(path, index=False)
    s3_path = None
    if upload_to_s3:
        s3_client, bucket = init_s3_client()
        ensure_bucket(s3_client, bucket)
        s3_client.upload_file(path, bucket, f"{prefix}/{filename}")
        s3_path = f"s3://{bucket}/{prefix}/{filename}"
    if uploaded_files is not None:
        uploaded_files.append({"type": prefix, "local_path": path, "s3_path": s3_path or "N/A", "rows": len(df)})


def save_batches(df, batch_size=5000, prefix="transactions", out_dir="data/raw_batches",
                 upload_to_s3=False, uploaded_files=None, batch_id=None):
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
        s3_path = None
        if upload_to_s3 and bucket:
            s3_client.upload_file(path, bucket, f"{prefix}/{filename}")
            s3_path = f"s3://{bucket}/{prefix}/{filename}"
        if uploaded_files is not None:
            uploaded_files.append({
                "type": prefix, "local_path": path,
                "s3_path": s3_path or "N/A",
                "rows": len(batch), "batch_id": batch_id, "batch_num": i
            })


# -----------------------------
# Main
# -----------------------------
def main(reset_seed=False):
    n_customers = random.randint(10, 60)
    n_terminals = random.randint(5, 50)
    window_minutes = 15
    batch_size = 20
    out_dir = "data/raw_batches"
    upload_to_s3 = True

    batch_id = str(uuid.uuid4())
    print(f"🚀 Starting Ghana Fraud Simulator | Batch ID: {batch_id}")
    print(f"Generating {n_customers} customers and {n_terminals} terminals...")

    avg_region_distances = compute_average_region_distances()
    customers = load_or_generate_customers(n_customers, reset=reset_seed)
    terminals = load_or_generate_terminals(n_terminals, reset=reset_seed)
    travel_profiles = generate_customer_travel_profiles(customers, avg_region_distances)
    transactions = generate_transactions(customers, terminals, avg_region_distances, travel_profiles,
                                         window_minutes=window_minutes)

    uploaded_files = []
    save_entity(customers, "customers", "customers", out_dir, upload_to_s3, uploaded_files)
    save_entity(terminals, "terminals", "terminals", out_dir, upload_to_s3, uploaded_files)
    save_entity(travel_profiles, "travel_profiles", "travel_profiles", out_dir, upload_to_s3, uploaded_files)
    save_batches(transactions, batch_size=batch_size, prefix="transactions", out_dir=out_dir,
                 upload_to_s3=upload_to_s3, uploaded_files=uploaded_files, batch_id=batch_id)

    record_metadata(uploaded_files, batch_id, output_dir=out_dir, upload_to_s3=upload_to_s3)
    print(" Simulation complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset-seed", action="store_true", help="Reset customer/terminal seeds")
    args = parser.parse_args()
    main(reset_seed=args.reset_seed)
