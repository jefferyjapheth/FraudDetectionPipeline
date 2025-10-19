# Credit Card Fraud Detection Pipeline - SCD2 Correction Summary

## Problem Identified
The pipeline incorrectly modeled **terminals** as SCD2 dimensions instead of **travel_profiles**, causing:
- Unnecessary version tracking for terminal locations (which are static)
- Loss of historical travel pattern changes (which should be tracked)
- Misaligned simulator seed generation and evolution logic

---

## Corrected Data Model

### ✅ SCD2 Dimensions (Track Historical Changes)
| Entity | Description | SCD2 Fields |
|--------|-------------|-------------|
| **dim_customers** | Customer profiles with behavioral metrics | version, effective_date, end_date, is_current |
| **dim_travel_profiles** | Customer travel patterns (changes over time) | version, effective_date, end_date, is_current |

### ✅ Static Dimensions (No Version Tracking)
| Entity | Description | Tracking Fields |
|--------|-------------|-----------------|
| **dim_terminals** | Terminal locations (rarely change) | BATCH_ID, created_at only |

### ✅ Facts (Append-Only)
| Entity | Description | Tracking Fields |
|--------|-------------|-----------------|
| **fact_transactions** | Transaction events | BATCH_ID, created_at only |

---

## Files Corrected

### 1. **ghana_fraud_simulator_scd2.py** (Simulator)
**Changes:**
- ❌ Removed `evolve_terminals()` function
- ❌ Removed version/effective_date from `generate_terminals()`
- ❌ Updated `load_or_generate_terminals()` to skip evolution
- ✅ Added `evolve_travel_profiles()` function
- ✅ Added version/effective_date to `generate_customer_travel_profiles()`
- ✅ Created `load_or_generate_travel_profiles()` with evolution support
- ✅ Updated documentation to reflect correct model

**Result:** Simulator now generates:
- Customers with SCD2 fields + evolution
- Terminals without SCD2 fields (static)
- Travel profiles with SCD2 fields + evolution

---

### 2. **creditCardFraudDetector.py** (DAG)
**Changes:**
- ✅ Updated documentation header (correct SCD2 model)
- ✅ Fixed `transform_chunk_with_scd2_init()`:
  - Changed condition from `["customers", "terminals"]` to `["customers", "travel_profiles"]`
  - Added explicit handling for terminals as static dimension
  - Removes any SCD2 fields that might exist in terminals data
- ✅ Updated `validate_data_quality_and_schema()` logging
- ✅ Updated `upsert_entity_to_iceberg()` documentation
- ✅ Updated all comments referencing SCD2 model

**Result:** Pipeline correctly:
- Adds SCD2 fields to customers and travel_profiles
- Excludes SCD2 fields from terminals
- Applies appropriate versioning logic per entity type

---

### 3. **transformation.py** (Schema Preparation)
**Changes:**
- ✅ Updated `SCHEMA_DEFINITIONS`:
  - **dim_terminals**: Removed version, effective_date, end_date, is_current
  - **dim_travel_profiles**: Added version, effective_date, end_date, is_current
  - All entities now include BATCH_ID and created_at
- ✅ Updated `prepare_for_iceberg()`:
  - Changed SCD2 condition from `["customers", "terminals"]` to `["customers", "travel_profiles"]`
  - Added explicit terminals handling to remove any SCD2 fields
  - Enhanced logging to show SCD2 status
- ✅ Updated `prepare_for_iceberg_with_arrow()` logging

**Result:** Schema enforcement correctly:
- Initializes SCD2 fields for customers and travel_profiles only
- Strips SCD2 fields from terminals data
- Ensures BATCH_ID tracking for all entity types

---

### 4. **scd2_handler.py** (SCD2 Logic)
**Changes:**
- ✅ Updated `apply_scd2_logic()`:
  - Changed table_map from `{"terminals": "dim_terminals"}` to `{"travel_profiles": "dim_travel_profiles"}`
  - Updated documentation to reflect correct model
  - Enhanced logging with file_type context
- ✅ Updated `get_scd2_config()`:
  - Removed terminals configuration
  - Added travel_profiles configuration:
    - Business keys: `["CUSTOMER_ID"]`
    - Compare columns: `["TRAVEL_REGIONS", "AVG_TRAVEL_DISTANCE_KM"]`
  - Added comment explaining terminals exclusion

**Result:** SCD2 comparison logic:
- Tracks customer attribute changes (location, spending patterns)
- Tracks travel profile changes (regions visited, average distances)
- Does NOT apply to terminals (static locations)

---

### 5. **iceberg_manager.py** (Table Schemas)
**Changes:**
- ✅ Updated `initialize_iceberg_tables()`:
  - **dim_customers**: Kept SCD2 fields (fields 9-12)
  - **dim_terminals**: Removed SCD2 fields (now only fields 1-7)
  - **dim_terminals**: Changed partition from effective_date to terminal_bucket
  - **dim_travel_profiles**: Added SCD2 fields (fields 4-7)
  - **dim_travel_profiles**: Added effective_date partition for time-travel
  - All tables include BATCH_ID and created_at
- ✅ Updated `upsert_to_iceberg_with_scd2()`:
  - Changed SCD2 condition from `["customers", "terminals"]` to `["customers", "travel_profiles"]`
  - Added explicit logging for static dimensions and facts
  - Enhanced status logging with SCD2 indicator

**Result:** Iceberg schemas correctly match:
- SCD2 dimensions have version tracking fields and date-based partitioning
- Static dimensions have bucket-based partitioning (no date fields)
- All tables track batch lineage via BATCH_ID

---

## Testing Checklist

### Simulator Testing
```bash
# Test seed generation
python src/ghana_fraud_simulator.py

# Verify files created:
# - data/seeds/customers_seed.csv (should have version, effective_date)
# - data/seeds/terminals_seed.csv (should NOT have version, effective_date)
# - data/seeds/travel_profiles_seed.csv (should have version, effective_date)
```

### DAG Testing
```bash
# Trigger DAG and monitor logs for:
# - "Processing terminals as static dimension (NO SCD2 fields)"
# - "Added SCD2 tracking fields to customers chunk"
# - "Added SCD2 tracking fields to travel_profiles chunk"
# - "SCD2: True" in upsert logs for customers/travel_profiles
# - "SCD2: False" in upsert logs for terminals/transactions
```

### Schema Validation
```sql
-- Check dim_terminals schema (should NOT have version fields)
DESCRIBE TABLE master_data.dim_terminals;

-- Check dim_travel_profiles schema (should have version fields)
DESCRIBE TABLE master_data.dim_travel_profiles;

-- Verify SCD2 tracking works
SELECT CUSTOMER_ID, version, effective_date, is_current 
FROM master_data.dim_travel_profiles 
ORDER BY CUSTOMER_ID, version;
```

---

## Benefits of Corrected Model

### 1. **Accurate Historical Tracking**
- Customer behavior changes (relocations, spending patterns) are versioned
- Travel pattern evolution is captured (new regions, changing distances)
- Terminal locations remain static (no unnecessary versioning)

### 2. **Performance Optimization**
- Terminals use bucket partitioning (faster lookups by TERMINAL_ID)
- SCD2 dimensions use date partitioning (efficient time-travel queries)
- Reduced storage overhead (no version history for static data)

### 3. **Fraud Detection Capabilities**
- Detect when customers start traveling to new regions (version changes in travel_profiles)
- Identify suspicious behavior patterns (sudden changes in travel_profiles)
- Compare current vs. historical customer spending patterns (version history in dim_customers)

### 4. **Data Lineage**
- BATCH_ID tracks which simulator run produced each record
- created_at timestamps record insertion time
- Full audit trail for compliance and debugging

---

## Migration Path (If Already Running)

### If you have existing data with incorrect SCD2 model:

#### Option 1: Clean Slate (Recommended for Development)
```bash
# Drop existing tables
# In Iceberg/Trino:
DROP TABLE IF EXISTS master_data.dim_customers;
DROP TABLE IF EXISTS master_data.dim_terminals;
DROP TABLE IF EXISTS master_data.dim_travel_profiles;
DROP TABLE IF EXISTS master_data.fact_transactions;

# Reset simulator seeds
python src/ghana_fraud_simulator.py --reset-seed

# Run DAG to rebuild with correct schemas
```

#### Option 2: Data Migration (For Production)
```python
# 1. Read existing terminals data
terminals_old = spark.read.table("master_data.dim_terminals")

# 2. Drop SCD2 columns
terminals_clean = terminals_old.drop("version", "effective_date", "end_date", "is_current")

# 3. Write to new table
terminals_clean.write.mode("overwrite").saveAsTable("master_data.dim_terminals_new")

# 4. Rename tables (atomic swap)
# ... similar process for travel_profiles
```

---

## Next Steps

1. ✅ **Deploy corrected simulator** - Replace old simulator script
2. ✅ **Deploy corrected DAG** - Update Airflow DAG file
3. ✅ **Deploy corrected modules** - Update transformation, scd2_handler, iceberg_manager
4. 🔄 **Clear existing Iceberg tables** - Drop and recreate with correct schemas
5. 🔄 **Reset simulator seeds** - Run with `--reset-seed` flag
6. 🔄 **Test end-to-end** - Trigger DAG and verify correct SCD2 behavior
7. 📊 **Verify queries** - Test time-travel queries on dim_travel_profiles

---

## Contact

If you encounter issues during migration, check:
- Airflow task logs for SCD2 processing messages
- Iceberg table schemas match corrected definitions
- Simulator seed files have correct columns
- No lingering SCD2 fields in terminals data