# Data Platform Setup

## Components
- **Database**: PostgreSQL - stores processed data
- **Processing**: Apache Airflow - runs data pipelines  
- **Storage**: MinIO - file storage (S3-compatible)
- **Admin**: pgAdmin - database management
- **Dashboards**: Metabase - charts and reports *(coming soon)*

## Quick Start

1. **Create environment file:**
```bash
cp .env.example .env
# Edit .env with your credentials
```


2. **Create Docker network:**
```bash
docker network create dataplatform_net
```

3. **Load environment variables:**
```bash
set -a && source .env && set +a
```

4. **Start all services:**
```bash
astro dev start --env .env --verbosity debug
```

## Environment Variables

Create a `.env` file with:
```bash
# Postgres
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=your_database_name
HOST_PORT=5432

# pgAdmin
PGADMIN_EMAIL=your_email@domain.com
PGADMIN_PASSWORD=your_pgadmin_password
PGADMIN_PORT=5050

# MinIO
MINIO_USER=your_minio_user
MINIO_PASSWORD=your_secure_minio_password
MINIO_BUCKET=your-bucket-name
```

## Access URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin/admin |
| pgAdmin | http://localhost:5050 | Use your PGADMIN_EMAIL/PASSWORD |
| MinIO Console | http://localhost:9001 | Use your MINIO_USER/PASSWORD |
| MinIO API | http://localhost:9000 | - |

## Services Status

✅ PostgreSQL (Database)  
✅ Apache Airflow (Processing)  
✅ MinIO (Storage)  
✅ pgAdmin (DB Admin)  
🔄 Metabase (Dashboards) - *in progress*

## Volumes
- `postgres-data`: PostgreSQL data persistence
- `pgadmin-data`: pgAdmin configuration
- `minio-data`: MinIO file storage

## Best Practices Implemented

### Docker Configuration
- **No hardcoded container names** - Prevents conflicts during scaling/redeployment
- **Environment variables in .env** - Centralizes configuration and keeps secrets out of code
- **Named volumes for persistence** - Data survives container restarts and updates
- **Custom bridge network** - Enables service discovery and isolation from default network
- **Health checks on critical services** - Ensures MinIO is ready before dependent services start
- **Restart policies** - Services auto-recover from failures (`on-failure` for stateful, `always` for stateless)

### Security & Operations
- **Database initialization script** - Automates schema setup on first run
- **Service dependencies** - MinIO client waits for MinIO health check before bucket creation
- **Public bucket policy** - Enables direct file access (adjust for production security needs)
- **Commented port mappings** - PostgreSQL not exposed externally (access via pgAdmin or internal network)

## Network
All services run on `dataplatform_net` bridge network for secure internal communication.


## Download csv data

1. Install the CLI locally once:

   ```bash
   pip install kaggle
   ```

2. Put your creds in `.env`:

   ```env
   KAGGLE_USERNAME=jamorant
   KAGGLE_KEY=8a677a97chjsdgfjsydyfalsidoe8e0d3a1baf797f3a
   ```

3. Run the one-liner:

   ```bash
   set -a && source .env && set +a && kaggle datasets download -d mlg-ulb/creditcardfraud -p ./data --unzip
   ```

That will drop `creditcard.csv` into `./data/`, and you can work with it


# Fraud Simulator

This project contains a Python script to generate **simulated fraud transaction data** and optionally upload it to a **MinIO/S3 bucket**. The simulation can generate transactions for a recent time window (e.g., last 15 minutes) with multiple fraud scenarios.

---

## Summary of `fraud_simulator.py`

- Generates customer and terminal profiles with random locations.
- Associates customers with nearby terminals.
- Simulates transactions with fraud scenarios:
  - **Baseline fraud:** random low-probability fraud.
  - **High-amount anomaly:** unusually large transaction amounts.
  - **Geographic fraud:** transactions on terminals far from the customer's usual area.
  - **Bursting fraud:** sequences of suspicious transactions in quick succession.
- Splits transactions into CSV **batches**.
- Saves batches to a **local folder** (default: `data/out_batches`).
- Optionally uploads the batches to a **MinIO/S3 bucket**.



## Run the Simulator

Generate transactions for the **last 15 minutes** and upload:

```bash
python src/fraud_simulator.py \
    --n-customers 500 \
    --n-terminals 1000 \
    --batch-size 5000 \
    --bucket $MINIO_BUCKET \
    --minio-endpoint http://localhost:9000 \
    --minio-access $MINIO_USER \
    --minio-secret $MINIO_PASSWORD \
    --upload \
    --window-minutes 15
```

* Default local output: `data/out_batches/`
* CSV batches are uploaded to the specified S3 bucket if `--upload` is used.

---

## Notes

* Increase `--n-customers` / `--n-terminals` for larger datasets.
* Adjust `--batch-size` to split transactions into smaller CSV files.
* The script randomly injects fraud using multiple scenarios.


