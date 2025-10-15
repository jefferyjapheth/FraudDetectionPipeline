#!/usr/bin/env python3
"""
MinIO Asset Event Service
-------------------------
Listens to MinIO bucket event notifications published to Kafka, identifies 
newly uploaded CSV files, and triggers corresponding asset events in Airflow.  

Core Responsibilities:
    - Consume Kafka events emitted by MinIO.
    - Identify valid dataset uploads (transactions, customers, terminals, etc.).
    - Trigger Airflow asset events for downstream data pipelines.
    - Maintain lightweight local cache of known Airflow asset URIs.
    - Provide health checks and structured logging for observability.

This service is designed for operational stability and idempotent event handling
in Airflow-based ingestion environments.
"""

import os
import sys
import json
import time
import signal
import logging
import urllib.parse
from datetime import datetime
from kafka import KafkaConsumer
import requests


# -----------------------------------------------------------------------------
# Logging Configuration
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MinIOAssetService")


# -----------------------------------------------------------------------------
# Service Definition
# -----------------------------------------------------------------------------
class MinIOAssetEventService:
    """
    Kafka consumer service that listens for MinIO object creation events and
    triggers Airflow asset updates accordingly.
    """

    FILE_TYPES = ["transactions", "customers", "terminals", "travel_profiles"]

    def __init__(self):
        """Initialize configuration, set up Kafka consumer, and preload asset cache."""
        self.running = True
        self.setup_signal_handlers()

        # Configuration (from environment)
        self.kafka_broker = os.getenv("KAFKA_BROKER_HOST", "localhost:9092")
        self.kafka_topic = os.getenv("MINIO_KAFKA_TOPIC", "minio-events")
        self.airflow_url = os.getenv("AIRFLOW_URL_HOST", "http://localhost:8080")
        self.raw_bucket = os.getenv("MINIO_BUCKET_RAW", "raw-data")
        self.health_check_interval = int(os.getenv("HEALTH_CHECK_INTERVAL", "300"))

        # Internal state and statistics
        self.asset_cache = {}
        self.last_health_check = time.time()
        self.events_processed = 0
        self.asset_events_created = 0
        self.errors_count = 0

        self.consumer = None

        self.initialize_consumer()
        self.load_assets()

        logger.info("Service initialized successfully")
        logger.info(f"Kafka: {self.kafka_broker} | Topic: {self.kafka_topic}")
        logger.info(f"Airflow: {self.airflow_url} | Raw Bucket: {self.raw_bucket}")
        logger.info(f"Cached {len(self.asset_cache)} assets at startup")

    # -------------------------------------------------------------------------
    # Graceful Shutdown Setup
    # -------------------------------------------------------------------------
    def setup_signal_handlers(self):
        """Attach handlers for SIGINT and SIGTERM to enable graceful shutdown."""

        def handle_signal(signum, _frame):
            logger.info(f"Received signal {signum}. Initiating shutdown...")
            self.running = False

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

    # -------------------------------------------------------------------------
    # Kafka Consumer Initialization
    # -------------------------------------------------------------------------
    def initialize_consumer(self):
        """Create Kafka consumer with retry and exponential backoff."""
        max_retries = 5
        delay = 10

        for attempt in range(max_retries):
            try:
                self.consumer = KafkaConsumer(
                    self.kafka_topic,
                    bootstrap_servers=[self.kafka_broker],
                    group_id="minio-airflow-asset-service",
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                    consumer_timeout_ms=1000,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000,
                )
                logger.info("Kafka consumer initialized successfully")
                return
            except Exception as e:
                logger.error(f"Kafka consumer init failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {delay}s...")
                    time.sleep(delay)
                    delay *= 2
                else:
                    logger.critical("Kafka initialization failed after max retries")
                    sys.exit(1)

    # -------------------------------------------------------------------------
    # Airflow Health Check
    # -------------------------------------------------------------------------
    def health_check(self):
        """Periodically verify connectivity to Airflow."""
        if time.time() - self.last_health_check < self.health_check_interval:
            return

        try:
            response = requests.get(f"{self.airflow_url}/api/v2/version", timeout=10)
            if response.status_code == 200:
                logger.info(
                    f"Health OK | Processed={self.events_processed} | "
                    f"Events={self.asset_events_created} | Errors={self.errors_count}"
                )
            else:
                logger.warning(f"Health check returned {response.status_code}")
        except Exception as e:
            logger.error(f"Health check failed: {e}")
        finally:
            self.last_health_check = time.time()

    # -------------------------------------------------------------------------
    # Asset Management (Airflow)
    # -------------------------------------------------------------------------
    def load_assets(self):
        """Load existing assets from Airflow API and populate cache."""
        try:
            response = requests.get(f"{self.airflow_url}/api/v2/assets", timeout=10)
            if response.status_code != 200:
                logger.warning(f"Failed to load assets (status {response.status_code})")
                return

            for asset in response.json().get("assets", []):
                uri = asset.get("uri")
                asset_id = asset.get("id")
                if uri and asset_id:
                    self.asset_cache[uri] = asset_id
            logger.info(f"Loaded {len(self.asset_cache)} assets into cache")
        except Exception as e:
            logger.error(f"Error loading assets: {e}")

    def get_asset_id(self, dataset_uri: str):
        """Resolve an asset URI to its Airflow asset ID."""
        if dataset_uri in self.asset_cache:
            return self.asset_cache[dataset_uri]

        try:
            response = requests.get(
                f"{self.airflow_url}/api/v2/assets",
                params={"uri_pattern": dataset_uri},
                timeout=10,
            )
            if response.status_code != 200:
                logger.error(f"Asset lookup failed for {dataset_uri}")
                return None

            assets = response.json().get("assets", [])
            if not assets:
                logger.warning(f"No asset found for {dataset_uri}")
                return None

            asset_id = assets[0]["id"]
            self.asset_cache[dataset_uri] = asset_id
            logger.info(f"Cached new asset mapping: {dataset_uri} → {asset_id}")
            return asset_id
        except Exception as e:
            logger.error(f"Error fetching asset ID for {dataset_uri}: {e}")
            return None

    def create_airflow_asset_event(self, dataset_uri: str, extra_data=None) -> bool:
        """Create an Airflow asset event entry for the given dataset."""
        asset_id = self.get_asset_id(dataset_uri)
        if not asset_id:
            self.errors_count += 1
            return False

        headers = {"Content-Type": "application/json"}
        payload = {"asset_id": asset_id, "extra": extra_data or {}}

        for attempt in range(3):
            try:
                response = requests.post(
                    f"{self.airflow_url}/api/v2/assets/events",
                    headers=headers,
                    json=payload,
                    timeout=30,
                )
                if response.status_code in (200, 201):
                    self.asset_events_created += 1
                    logger.info(f"Asset event created for {dataset_uri} (ID: {asset_id})")
                    return True
                logger.error(f"Asset event creation failed ({response.status_code}): {response.text}")
            except Exception as e:
                logger.error(f"Error creating asset event (attempt {attempt + 1}/3): {e}")
            time.sleep(2 ** attempt)

        self.errors_count += 1
        return False

    # -------------------------------------------------------------------------
    # Event Handling
    # -------------------------------------------------------------------------
    @classmethod
    def get_file_type(cls, object_key: str):
        """Determine dataset type from S3 key prefix."""
        for ft in cls.FILE_TYPES:
            if object_key.startswith(f"{ft}/") and object_key.endswith(".csv"):
                return ft
        return None

    @staticmethod
    def is_relevant_event(event_name: str):
        """Identify if an event corresponds to an object creation."""
        return event_name.startswith("s3:ObjectCreated:")

    def process_record(self, record: dict):
        """Process a single MinIO event record."""
        s3_info = record.get("s3", {})
        bucket_name = s3_info.get("bucket", {}).get("name", "")
        if bucket_name != self.raw_bucket:
            return False

        # Normalize key (remove redundant prefixes)
        object_key = urllib.parse.unquote(s3_info.get("object", {}).get("key", ""))
        if object_key.startswith(f"{bucket_name}/"):
            object_key = object_key[len(bucket_name) + 1:]

        file_type = self.get_file_type(object_key)
        if not file_type:
            return False

        metadata = {
            "file_key": object_key,
            "file_path": f"s3://{bucket_name}/{object_key}",
            "file_size_bytes": s3_info.get("object", {}).get("size", 0),
            "bucket_name": bucket_name,
            "file_type": file_type,
            "event_name": record.get("EventName", ""),
            "processed_at": datetime.utcnow().isoformat(),
            "source": "minio_kafka_notification",
        }

        dataset_uri = f"s3://{bucket_name}/creditcard_files"
        return self.create_airflow_asset_event(dataset_uri, metadata)

    def process_minio_event(self, message):
        """Process and route a MinIO Kafka message."""
        try:
            self.events_processed += 1
            event_data = message.value
            if not isinstance(event_data, dict):
                logger.warning(f"Invalid event format: {event_data}")
                return

            event_name = event_data.get("EventName", "")
            if not self.is_relevant_event(event_name):
                return

            records = event_data.get("Records", [])
            processed_files = sum(self.process_record(record) for record in records)
            if processed_files:
                logger.info(f"Processed {processed_files} file(s) for event {event_name}")

        except Exception as e:
            self.errors_count += 1
            logger.error(f"Error processing MinIO event: {e}")

    # -------------------------------------------------------------------------
    # Main Service Loop
    # -------------------------------------------------------------------------
    def start_listening(self):
        """Main loop polling Kafka and processing MinIO events."""
        logger.info("Starting event listener... Press Ctrl+C to stop.")

        try:
            while self.running:
                try:
                    message_batch = self.consumer.poll(timeout_ms=1000)
                    for messages in message_batch.values():
                        for message in messages:
                            if not self.running:
                                break
                            self.process_minio_event(message)
                    self.health_check()
                except Exception as e:
                    logger.error(f"Error in main loop: {e}")
                    self.errors_count += 1
                    time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            self.shutdown()

    # -------------------------------------------------------------------------
    # Shutdown
    # -------------------------------------------------------------------------
    def shutdown(self):
        """Release resources and log final metrics."""
        logger.info("Shutting down service gracefully...")
        logger.info(
            f"Final Stats — Processed: {self.events_processed} | "
            f"Created: {self.asset_events_created} | Errors: {self.errors_count}"
        )
        if self.consumer:
            self.consumer.close()
        logger.info("Shutdown complete.")


# -----------------------------------------------------------------------------
# Entry Point
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        service = MinIOAssetEventService()
        service.start_listening()
    except Exception as e:
        logger.critical(f"Fatal error during startup: {e}")
        sys.exit(1)
