#!/usr/bin/env python3
"""
MinIO Asset Event Service - Refactored for clarity and reduced cognitive complexity.
"""

import os
import json
import logging
import requests
import signal
import sys
import time
import urllib.parse
from kafka import KafkaConsumer
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('MinIOAssetService')


class MinIOAssetEventService:
    """
    Consumes Kafka events from MinIO and triggers dataset updates in Airflow.
    """

    FILE_TYPES = ['transactions', 'customers', 'terminals', 'travel_profiles']

    def __init__(self):
        """Initialize the service, load configuration, and set up resources."""
        self.running = True
        self.setup_signal_handlers()

        # Configuration
        self.kafka_broker = os.getenv('KAFKA_BROKER_HOST', 'localhost:9092')
        self.minio_kafka_topic = os.getenv('MINIO_KAFKA_TOPIC', 'minio-events')
        self.airflow_url = os.getenv('AIRFLOW_URL_HOST', 'http://localhost:8080')
        self.raw_bucket = os.getenv('MINIO_BUCKET_RAW', 'raw-data')
        self.health_check_interval = int(os.getenv('HEALTH_CHECK_INTERVAL', '300'))
        self.last_health_check = time.time()

        # Cache for asset URI to ID mapping
        self.asset_cache = {}

        # Statistics
        self.events_processed = 0
        self.asset_events_created = 0
        self.errors_count = 0

        self.consumer = None
        self.initialize_consumer()
        self.load_assets()

        logger.info("MinIO Asset Event Service initialized")
        logger.info(f"Kafka: {self.kafka_broker} | Topic: {self.minio_kafka_topic}")
        logger.info(f"Airflow: {self.airflow_url} | Bucket: {self.raw_bucket}")
        logger.info(f"Cached assets: {list(self.asset_cache.keys())}")

    # ------------------- Setup ------------------- #
    def setup_signal_handlers(self):
        """Setup graceful shutdown handlers for SIGINT and SIGTERM."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}. Shutting down gracefully...")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    # ------------------- Kafka ------------------- #
    def initialize_consumer(self):
        """Initialize Kafka consumer with retry and exponential backoff."""
        max_retries = 5
        retry_delay = 10
        for attempt in range(max_retries):
            try:
                self.consumer = KafkaConsumer(
                    self.minio_kafka_topic,
                    bootstrap_servers=[self.kafka_broker],
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    group_id='minio-airflow-asset-service',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    consumer_timeout_ms=1000,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000
                )
                logger.info("Kafka consumer initialized successfully")
                return
            except Exception as e:
                logger.error(f"Failed to initialize Kafka consumer (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error("Max retries exceeded. Exiting.")
                    sys.exit(1)

    # ------------------- Health Check ------------------- #
    def health_check(self):
        """Perform periodic health checks."""
        if time.time() - self.last_health_check <= self.health_check_interval:
            return
        try:
            response = requests.get(f"{self.airflow_url}/api/v2/version", timeout=10)
            if response.status_code == 200:
                logger.info(
                    f"Health check OK | Processed: {self.events_processed} | "
                    f"Created: {self.asset_events_created} | Errors: {self.errors_count}")
            else:
                logger.warning(f"Health check warning: {response.status_code}")
        except Exception as e:
            logger.error(f"Health check failed: {e}")
        finally:
            self.last_health_check = time.time()

    # ------------------- Asset Management ------------------- #
    def load_assets(self):
        """Load all assets from Airflow and cache the URI to ID mapping."""
        try:
            response = requests.get(f"{self.airflow_url}/api/v2/assets", timeout=10)
            if response.status_code != 200:
                logger.warning(f"Failed to load assets: {response.status_code}")
                return
            assets_data = response.json()
            for asset in assets_data.get('assets', []):
                uri = asset.get('uri')
                asset_id = asset.get('id')
                if uri and asset_id:
                    self.asset_cache[uri] = asset_id
                    logger.info(f"Cached asset: {uri} -> ID {asset_id}")
        except Exception as e:
            logger.error(f"Error loading assets: {e}")

    def get_asset_id(self, dataset_uri):
        """Get asset ID from cache or fetch from API."""
        if dataset_uri in self.asset_cache:
            return self.asset_cache[dataset_uri]

        try:
            response = requests.get(
                f"{self.airflow_url}/api/v2/assets",
                params={"uri_pattern": dataset_uri},
                timeout=10
            )
            if response.status_code != 200:
                logger.error(f"Asset not found for URI: {dataset_uri}")
                return None

            assets = response.json().get('assets', [])
            if not assets:
                logger.error(f"Asset not found for URI: {dataset_uri}")
                return None

            asset_id = assets[0]['id']
            self.asset_cache[dataset_uri] = asset_id
            logger.info(f"Cached new asset: {dataset_uri} -> ID {asset_id}")
            return asset_id
        except Exception as e:
            logger.error(f"Error fetching asset ID: {e}")
            return None

    def create_airflow_asset_event(self, dataset_uri, extra_data=None):
        """Create an asset event in Airflow using asset_id."""
        asset_id = self.get_asset_id(dataset_uri)
        if not asset_id:
            logger.error(f"Cannot create event: asset ID not found for {dataset_uri}")
            self.errors_count += 1
            return False

        headers = {"Content-Type": "application/json"}
        max_retries = 3

        for attempt in range(max_retries):
            try:
                payload = {"asset_id": asset_id, "extra": extra_data or {}}
                response = requests.post(
                    f"{self.airflow_url}/api/v2/assets/events",
                    headers=headers,
                    json=payload,
                    timeout=30
                )
                if response.status_code in [200, 201]:
                    self.asset_events_created += 1
                    logger.info(f"Created asset event for {dataset_uri} (ID: {asset_id})")
                    return True
                logger.error(f"Failed to create asset event: {response.status_code} - {response.text}")
            except Exception as e:
                logger.error(f"Error creating asset event (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(2 ** attempt)

        self.errors_count += 1
        return False

    # ------------------- Event Processing ------------------- #
    @classmethod
    def get_file_type(cls, object_key: str):
        for ft in cls.FILE_TYPES:
            if object_key.startswith(f"{ft}/") and object_key.endswith('.csv'):
                return ft
        return None

    @staticmethod
    def is_relevant_event(event_name: str):
        return event_name.startswith('s3:ObjectCreated:')

    def process_record(self, record):
        s3_info = record.get('s3', {})
        bucket_name = s3_info.get('bucket', {}).get('name', '')
        if bucket_name != self.raw_bucket:
            return False

        object_key = urllib.parse.unquote(s3_info.get('object', {}).get('key', ''))
        if object_key.startswith(f"{bucket_name}/"):
            object_key = object_key[len(bucket_name) + 1:]

        file_type = self.get_file_type(object_key)
        if not file_type:
            return False

        metadata = {
            'file_key': object_key,
            'file_path': f"s3://{bucket_name}/{object_key}",
            'file_size_bytes': s3_info.get('object', {}).get('size', 0),
            'bucket_name': bucket_name,
            'file_type': file_type,
            'event_name': record.get('EventName', ''),
            'processed_at': datetime.now().isoformat(),
            'source': 'minio_kafka_notification'
        }

        return self.create_airflow_asset_event(f"s3://{bucket_name}/creditcard_files", metadata)

    def process_minio_event(self, message):
        try:
            self.events_processed += 1
            event_data = message.value
            if not isinstance(event_data, dict):
                logger.warning(f"Received non-dict message: {event_data}")
                return

            event_name = event_data.get('EventName', '')
            if not self.is_relevant_event(event_name):
                return

            records = event_data.get('Records', [])
            processed_files = sum(self.process_record(record) for record in records)

            if processed_files:
                logger.info(f"Successfully processed {processed_files} file(s) from event")

        except Exception as e:
            self.errors_count += 1
            logger.error(f"Error processing MinIO event: {e}")

    # ------------------- Service Loop ------------------- #
    def start_listening(self):
        """Main service loop to poll Kafka for messages."""
        logger.info("Starting MinIO Asset Event Service...")
        logger.info("Listening for file uploads... Press Ctrl+C to stop")

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
            logger.info("Received interrupt signal")
        finally:
            self.shutdown()

    # ------------------- Shutdown ------------------- #
    def shutdown(self):
        """Clean shutdown procedure."""
        logger.info("Shutting down service...")
        logger.info(
            f"Final stats - Processed: {self.events_processed} | "
            f"Created: {self.asset_events_created} | Errors: {self.errors_count}")
        if self.consumer:
            self.consumer.close()
        logger.info("Service stopped cleanly")


if __name__ == "__main__":
    try:
        service = MinIOAssetEventService()
        service.start_listening()
    except Exception as e:
        logger.error(f"Service failed to start: {e}")
        sys.exit(1)
