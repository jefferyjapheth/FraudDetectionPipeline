"""
ghana_fraud_simulator_raw_travel_s3_env.py

Simulates raw credit card transaction data for Ghana and uploads to MinIO/S3.
"""

import os
import uuid
import math
import random
import pandas as pd
import numpy as np
import boto3
from botocore.exceptions import ClientError

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
    return math.sqrt(dx*dx + dy*dy)

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
# Generators
# -----------------------------
def generate_customers(n_customers=500, seed=0):
    np.random.seed(seed)
    customers = []
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
            "mean_nb_tx_per_day": mean_tx_day
        })
    return pd.DataFrame(customers)

def generate_terminals(n_terminals=1000, seed=1):
    np.random.seed(seed)
    terminals = []
    for i in range(n_terminals):
        city_info = random.choice(GHANA_CITIES)
        terminals.append({
            "TERMINAL_ID": i,
            "CITY": city_info["city"],
            "REGION": city_info["region"],
            "x_terminal_id": city_info["x"] + np.random.normal(0, 5),
            "y_terminal_id": city_info["y"] + np.random.normal(0, 5),
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

# -----------------------------
# Transactions
# -----------------------------
def generate_transactions(customers, terminals, avg_region_distances,
                          travel_profiles, window_minutes=60,
                          p_high_amount=0.05, p_burst=0.02):
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
                amount = amount * np.random.uniform(3, 8)

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
# MinIO/S3 helpers
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
        print(f"Bucket {bucket_name} already exists")
    except ClientError as e:
        if e.response['Error']['Code'] in ['404', 'NoSuchBucket']:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Created bucket {bucket_name}")
        else:
            raise e

from datetime import datetime

# -----------------------------
# Save Helper
# -----------------------------
def save_entity(df, name, prefix, out_dir="data/raw_batches", upload_to_s3=False):
    # Ensure the prefix matches what the Asset Event Service expects
    valid_prefixes = ["customers", "terminals", "transactions", "travel_profiles"]
    minio_prefix = prefix if prefix in valid_prefixes else prefix

    os.makedirs(os.path.join(out_dir, minio_prefix), exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{name}_{timestamp}.csv"
    path = os.path.join(out_dir, minio_prefix, filename)
    df.to_csv(path, index=False)
    print(f"Saved {path} ({len(df)} rows)")

    if upload_to_s3:
        s3_client, bucket = init_s3_client()
        ensure_bucket(s3_client, bucket)
        # Upload to MinIO using the correct prefix folder
        s3_client.upload_file(path, bucket, f"{minio_prefix}/{filename}")
        print(f"Uploaded {filename} to s3://{bucket}/{minio_prefix}/{filename}")


def save_batches(df, batch_size=5000, prefix="transactions",
                 out_dir="data/raw_batches", upload_to_s3=False):
    valid_prefixes = ["customers", "terminals", "transactions"]
    minio_prefix = prefix if prefix in valid_prefixes else prefix

    os.makedirs(os.path.join(out_dir, minio_prefix), exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_client, bucket = (init_s3_client() if upload_to_s3 else (None, None))
    if upload_to_s3 and bucket:
        ensure_bucket(s3_client, bucket)

    for i, start in enumerate(range(0, len(df), batch_size), start=1):
        batch = df.iloc[start:start + batch_size]
        filename = f"{minio_prefix}_{timestamp}_batch_{i:03d}.csv"
        path = os.path.join(out_dir, minio_prefix, filename)
        batch.to_csv(path, index=False)
        print(f"Saved {path} ({len(batch)} rows)")
        if upload_to_s3 and bucket:
            s3_client.upload_file(path, bucket, f"{minio_prefix}/{filename}")
            print(f"Uploaded {filename} to s3://{bucket}/{minio_prefix}/{filename}")

# -----------------------------
# Main
# -----------------------------
def main():
    n_customers = 1000
    n_terminals = 1500
    window_minutes = 15
    batch_size = 1000
    out_dir = "data/raw_batches"
    upload_to_s3 = True

    avg_region_distances = compute_average_region_distances()
    customers = generate_customers(n_customers)
    terminals = generate_terminals(n_terminals)
    travel_profiles = generate_customer_travel_profiles(customers, avg_region_distances)
    transactions = generate_transactions(customers, terminals, avg_region_distances, travel_profiles,
                                         window_minutes=window_minutes)

    # Save all entities
    save_entity(customers, "customers", "customers", out_dir, upload_to_s3)
    save_entity(terminals, "terminals", "terminals", out_dir, upload_to_s3)
    save_entity(travel_profiles, "travel_profiles", "travel_profiles", out_dir, upload_to_s3)
    save_batches(transactions, batch_size=batch_size, prefix="transactions",
                 out_dir=out_dir, upload_to_s3=upload_to_s3)

if __name__ == "__main__":
    main()
