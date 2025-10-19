-- PostgreSQL OLAP Schema Initialization
-- Creates analytics-ready tables for Metabase consumption
-- Auto-runs on first PostgreSQL container startup
--
-- CORRECTED SCHEMA MODEL:
-- - dim_customers_current: WITH SCD2 fields (version, effective_date)
-- - dim_terminals_current: WITHOUT SCD2 fields (static dimension)
-- - dim_travel_profiles_current: WITH SCD2 fields (NEW TABLE)
-- - fact_transactions_enriched: Denormalized with current dimension snapshots

-- Create schema
CREATE SCHEMA IF NOT EXISTS creditcard_analytics;

-- =====================================================================
--  Dimension: Current Customers (SCD2)
-- =====================================================================

CREATE TABLE IF NOT EXISTS creditcard_analytics.dim_customers_current (
    customer_id BIGINT PRIMARY KEY,
    home_city VARCHAR(255),
    home_region VARCHAR(255),
    x_customer_id DOUBLE PRECISION,
    y_customer_id DOUBLE PRECISION,
    mean_amount DOUBLE PRECISION,
    std_amount DOUBLE PRECISION,
    mean_nb_tx_per_day DOUBLE PRECISION,
    -- SCD2 tracking fields
    version BIGINT,
    effective_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN,
    -- Batch tracking
    batch_id VARCHAR(255),
    created_at TIMESTAMP,
    -- Audit column
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_customers_region ON creditcard_analytics.dim_customers_current(home_region);
CREATE INDEX IF NOT EXISTS idx_customers_city ON creditcard_analytics.dim_customers_current(home_city);
CREATE INDEX IF NOT EXISTS idx_customers_is_current ON creditcard_analytics.dim_customers_current(is_current) WHERE is_current = TRUE;

COMMENT ON TABLE creditcard_analytics.dim_customers_current IS 
'Current customer dimension (SCD2). Filtered to is_current = TRUE. Tracks customer profile changes over time.';

-- =====================================================================
--  Dimension: Current Terminals (Static - NO SCD2)
-- =====================================================================

CREATE TABLE IF NOT EXISTS creditcard_analytics.dim_terminals_current (
    terminal_id BIGINT PRIMARY KEY,
    city VARCHAR(255),
    region VARCHAR(255),
    x_terminal_id DOUBLE PRECISION,
    y_terminal_id DOUBLE PRECISION,
    -- NO SCD2 fields (version, effective_date, end_date, is_current)
    -- Only batch tracking
    batch_id VARCHAR(255),
    created_at TIMESTAMP,
    -- Audit column
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_terminals_region ON creditcard_analytics.dim_terminals_current(region);
CREATE INDEX IF NOT EXISTS idx_terminals_city ON creditcard_analytics.dim_terminals_current(city);

COMMENT ON TABLE creditcard_analytics.dim_terminals_current IS 
'Static terminal dimension. No version tracking. Terminals are physical locations that rarely change.';

-- =====================================================================
--  Dimension: Current Travel Profiles (SCD2)
-- =====================================================================

CREATE TABLE IF NOT EXISTS creditcard_analytics.dim_travel_profiles_current (
    customer_id BIGINT PRIMARY KEY,
    travel_regions TEXT,
    avg_travel_distance_km DOUBLE PRECISION,
    -- SCD2 tracking fields
    version BIGINT,
    effective_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN,
    -- Batch tracking
    batch_id VARCHAR(255),
    created_at TIMESTAMP,
    -- Audit column
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Foreign key to customers
    CONSTRAINT fk_travel_customer FOREIGN KEY (customer_id) 
        REFERENCES creditcard_analytics.dim_customers_current(customer_id)
);

CREATE INDEX IF NOT EXISTS idx_travel_customer ON creditcard_analytics.dim_travel_profiles_current(customer_id);
CREATE INDEX IF NOT EXISTS idx_travel_is_current ON creditcard_analytics.dim_travel_profiles_current(is_current) WHERE is_current = TRUE;

COMMENT ON TABLE creditcard_analytics.dim_travel_profiles_current IS 
'Current travel profile dimension (SCD2). Filtered to is_current = TRUE. Tracks changing customer travel patterns.';

-- =====================================================================
--  Fact: Enriched Transactions (Denormalized Star Schema)
-- =====================================================================

CREATE TABLE IF NOT EXISTS creditcard_analytics.fact_transactions_enriched (
    transaction_id VARCHAR(255) PRIMARY KEY,
    tx_datetime TIMESTAMP NOT NULL,
    tx_date DATE NOT NULL,
    tx_amount DOUBLE PRECISION NOT NULL,
    customer_id BIGINT,
    terminal_id BIGINT,
    tx_time_seconds BIGINT,
    tx_time_days BIGINT,
    
    -- Transaction-time snapshots (pre-enriched by simulator)
    -- These capture the state at transaction time, not current state
    tx_terminal_city VARCHAR(255),
    tx_terminal_region VARCHAR(255),
    distance_from_home_km DOUBLE PRECISION,
    tx_customer_travel_regions TEXT,
    tx_is_new_region BOOLEAN,
    tx_avg_region_distance_km DOUBLE PRECISION,
    
    -- Current customer profile (from dim_customers_current)
    -- These are point-in-time snapshots when data is loaded to OLAP
    customer_home_city VARCHAR(255),
    customer_home_region VARCHAR(255),
    customer_x_coord DOUBLE PRECISION,
    customer_y_coord DOUBLE PRECISION,
    customer_mean_amount DOUBLE PRECISION,
    customer_std_amount DOUBLE PRECISION,
    customer_mean_nb_tx_per_day DOUBLE PRECISION,
    customer_version BIGINT,
    customer_effective_date TIMESTAMP,
    
    -- Current travel profile (from dim_travel_profiles_current)
    customer_travel_regions TEXT,
    customer_avg_travel_distance_km DOUBLE PRECISION,
    customer_travel_version BIGINT,
    customer_travel_effective_date TIMESTAMP,
    
    -- Current terminal data (from dim_terminals_current)
    terminal_current_city VARCHAR(255),
    terminal_current_region VARCHAR(255),
    terminal_x_coord DOUBLE PRECISION,
    terminal_y_coord DOUBLE PRECISION,
    
    -- Calculated fraud indicators
    amount_deviation_from_mean DOUBLE PRECISION,
    is_unusual_amount BOOLEAN,
    is_cross_region BOOLEAN,
    is_far_from_home BOOLEAN,
    is_travel_pattern_mismatch BOOLEAN,
    
    -- Audit columns
    batch_id VARCHAR(255),
    created_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign keys
    CONSTRAINT fk_tx_customer FOREIGN KEY (customer_id) 
        REFERENCES creditcard_analytics.dim_customers_current(customer_id),
    CONSTRAINT fk_tx_terminal FOREIGN KEY (terminal_id) 
        REFERENCES creditcard_analytics.dim_terminals_current(terminal_id)
);

-- Performance indexes for common Metabase queries
CREATE INDEX IF NOT EXISTS idx_tx_date ON creditcard_analytics.fact_transactions_enriched(tx_date DESC);
CREATE INDEX IF NOT EXISTS idx_tx_customer ON creditcard_analytics.fact_transactions_enriched(customer_id);
CREATE INDEX IF NOT EXISTS idx_tx_terminal ON creditcard_analytics.fact_transactions_enriched(terminal_id);
CREATE INDEX IF NOT EXISTS idx_tx_datetime ON creditcard_analytics.fact_transactions_enriched(tx_datetime DESC);
CREATE INDEX IF NOT EXISTS idx_tx_batch ON creditcard_analytics.fact_transactions_enriched(batch_id);

-- Fraud indicator indexes
CREATE INDEX IF NOT EXISTS idx_tx_unusual_amount ON creditcard_analytics.fact_transactions_enriched(is_unusual_amount) WHERE is_unusual_amount = TRUE;
CREATE INDEX IF NOT EXISTS idx_tx_cross_region ON creditcard_analytics.fact_transactions_enriched(is_cross_region) WHERE is_cross_region = TRUE;
CREATE INDEX IF NOT EXISTS idx_tx_far_from_home ON creditcard_analytics.fact_transactions_enriched(is_far_from_home) WHERE is_far_from_home = TRUE;
CREATE INDEX IF NOT EXISTS idx_tx_travel_mismatch ON creditcard_analytics.fact_transactions_enriched(is_travel_pattern_mismatch) WHERE is_travel_pattern_mismatch = TRUE;

-- Geographic indexes
CREATE INDEX IF NOT EXISTS idx_tx_customer_region ON creditcard_analytics.fact_transactions_enriched(customer_home_region);
CREATE INDEX IF NOT EXISTS idx_tx_terminal_region ON creditcard_analytics.fact_transactions_enriched(tx_terminal_region);

-- Version tracking indexes (for SCD2 analysis)
CREATE INDEX IF NOT EXISTS idx_tx_customer_version ON creditcard_analytics.fact_transactions_enriched(customer_version);
CREATE INDEX IF NOT EXISTS idx_tx_travel_version ON creditcard_analytics.fact_transactions_enriched(customer_travel_version);

COMMENT ON TABLE creditcard_analytics.fact_transactions_enriched IS 
'Denormalized transaction fact table with current dimension attributes and fraud indicators. 
Includes both transaction-time snapshots (from simulator) and current dimension states (from OLAP load). 
Optimized for Metabase analytics with fraud detection focus.';

-- =====================================================================
--  Historical View: Customer Changes
-- =====================================================================

CREATE TABLE IF NOT EXISTS creditcard_analytics.dim_customers_history (
    customer_id BIGINT,
    home_city VARCHAR(255),
    home_region VARCHAR(255),
    x_customer_id DOUBLE PRECISION,
    y_customer_id DOUBLE PRECISION,
    mean_amount DOUBLE PRECISION,
    std_amount DOUBLE PRECISION,
    mean_nb_tx_per_day DOUBLE PRECISION,
    version BIGINT,
    effective_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN,
    batch_id VARCHAR(255),
    created_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer_id, version)
);

CREATE INDEX IF NOT EXISTS idx_customers_hist_id ON creditcard_analytics.dim_customers_history(customer_id);
CREATE INDEX IF NOT EXISTS idx_customers_hist_version ON creditcard_analytics.dim_customers_history(version);
CREATE INDEX IF NOT EXISTS idx_customers_hist_effective ON creditcard_analytics.dim_customers_history(effective_date);

COMMENT ON TABLE creditcard_analytics.dim_customers_history IS 
'Full history of customer dimension changes. All versions from SCD2 tracking.';

-- =====================================================================
--  Historical View: Travel Profile Changes
-- =====================================================================

CREATE TABLE IF NOT EXISTS creditcard_analytics.dim_travel_profiles_history (
    customer_id BIGINT,
    travel_regions TEXT,
    avg_travel_distance_km DOUBLE PRECISION,
    version BIGINT,
    effective_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN,
    batch_id VARCHAR(255),
    created_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer_id, version)
);

CREATE INDEX IF NOT EXISTS idx_travel_hist_id ON creditcard_analytics.dim_travel_profiles_history(customer_id);
CREATE INDEX IF NOT EXISTS idx_travel_hist_version ON creditcard_analytics.dim_travel_profiles_history(version);
CREATE INDEX IF NOT EXISTS idx_travel_hist_effective ON creditcard_analytics.dim_travel_profiles_history(effective_date);

COMMENT ON TABLE creditcard_analytics.dim_travel_profiles_history IS 
'Full history of travel profile changes. All versions from SCD2 tracking. Useful for detecting pattern evolution.';

-- =====================================================================
--  Summary View: Daily Fraud Statistics
-- =====================================================================

CREATE OR REPLACE VIEW creditcard_analytics.vw_daily_fraud_summary AS
SELECT 
    tx_date,
    COUNT(*) as total_transactions,
    SUM(tx_amount) as total_amount,
    AVG(tx_amount) as avg_amount,
    STDDEV(tx_amount) as stddev_amount,
    MIN(tx_amount) as min_amount,
    MAX(tx_amount) as max_amount,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT terminal_id) as unique_terminals,
    -- Fraud indicators
    SUM(CASE WHEN is_unusual_amount THEN 1 ELSE 0 END) as unusual_amount_count,
    SUM(CASE WHEN is_cross_region THEN 1 ELSE 0 END) as cross_region_count,
    SUM(CASE WHEN is_far_from_home THEN 1 ELSE 0 END) as far_from_home_count,
    SUM(CASE WHEN is_travel_pattern_mismatch THEN 1 ELSE 0 END) as travel_mismatch_count,
    -- Fraud rates
    ROUND(100.0 * SUM(CASE WHEN is_unusual_amount THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as unusual_amount_pct,
    ROUND(100.0 * SUM(CASE WHEN is_cross_region THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as cross_region_pct,
    ROUND(100.0 * SUM(CASE WHEN is_far_from_home THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as far_from_home_pct,
    ROUND(100.0 * SUM(CASE WHEN is_travel_pattern_mismatch THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as travel_mismatch_pct,
    -- Geographic metrics
    AVG(distance_from_home_km) as avg_distance_from_home,
    MAX(distance_from_home_km) as max_distance_from_home
FROM creditcard_analytics.fact_transactions_enriched
GROUP BY tx_date
ORDER BY tx_date DESC;

COMMENT ON VIEW creditcard_analytics.vw_daily_fraud_summary IS 
'Daily aggregation of fraud indicators and transaction metrics for trend analysis.';

-- =====================================================================
--  Summary View: Regional Risk Profile
-- =====================================================================

CREATE OR REPLACE VIEW creditcard_analytics.vw_regional_risk_profile AS
SELECT 
    customer_home_region as region,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(*) as total_transactions,
    SUM(tx_amount) as total_amount,
    AVG(tx_amount) as avg_amount,
    AVG(distance_from_home_km) as avg_distance_from_home,
    -- Fraud metrics
    SUM(CASE WHEN is_unusual_amount THEN 1 ELSE 0 END) as unusual_amount_count,
    SUM(CASE WHEN is_cross_region THEN 1 ELSE 0 END) as cross_region_count,
    SUM(CASE WHEN is_far_from_home THEN 1 ELSE 0 END) as far_from_home_count,
    SUM(CASE WHEN is_travel_pattern_mismatch THEN 1 ELSE 0 END) as travel_mismatch_count,
    -- Combined fraud score
    ROUND(100.0 * (
        SUM(CASE WHEN is_unusual_amount THEN 1 ELSE 0 END) +
        SUM(CASE WHEN is_cross_region THEN 1 ELSE 0 END) +
        SUM(CASE WHEN is_far_from_home THEN 1 ELSE 0 END) +
        SUM(CASE WHEN is_travel_pattern_mismatch THEN 1 ELSE 0 END)
    ) / NULLIF(COUNT(*), 0), 2) as combined_fraud_rate_pct
FROM creditcard_analytics.fact_transactions_enriched
GROUP BY customer_home_region
ORDER BY combined_fraud_rate_pct DESC;

COMMENT ON VIEW creditcard_analytics.vw_regional_risk_profile IS 
'Aggregated fraud statistics by customer home region. Shows regional risk patterns.';

-- =====================================================================
--  Summary View: Customer Travel Pattern Evolution
-- =====================================================================

CREATE OR REPLACE VIEW creditcard_analytics.vw_customer_travel_evolution AS
SELECT 
    c.customer_id,
    c.home_city,
    c.home_region,
    c.version as customer_version,
    tp.version as travel_version,
    tp.travel_regions,
    tp.avg_travel_distance_km,
    tp.effective_date as travel_effective_date,
    tp.end_date as travel_end_date,
    tp.is_current as is_current_travel_pattern,
    -- Calculate how many regions customer travels to
    array_length(string_to_array(tp.travel_regions, ','), 1) as num_travel_regions
FROM creditcard_analytics.dim_customers_current c
LEFT JOIN creditcard_analytics.dim_travel_profiles_history tp 
    ON c.customer_id = tp.customer_id
WHERE c.is_current = TRUE
ORDER BY c.customer_id, tp.version DESC;

COMMENT ON VIEW creditcard_analytics.vw_customer_travel_evolution IS 
'Shows how customer travel patterns evolve over time. Useful for detecting behavioral changes that may indicate fraud.';

-- =====================================================================
--  Summary View: Version Change Analysis
-- =====================================================================

CREATE OR REPLACE VIEW creditcard_analytics.vw_dimension_version_changes AS
SELECT 
    'customers' as dimension_type,
    customer_id as entity_id,
    version,
    effective_date,
    end_date,
    is_current,
    EXTRACT(EPOCH FROM (COALESCE(end_date, CURRENT_TIMESTAMP) - effective_date)) / 86400 as days_valid
FROM creditcard_analytics.dim_customers_history
UNION ALL
SELECT 
    'travel_profiles' as dimension_type,
    customer_id as entity_id,
    version,
    effective_date,
    end_date,
    is_current,
    EXTRACT(EPOCH FROM (COALESCE(end_date, CURRENT_TIMESTAMP) - effective_date)) / 86400 as days_valid
FROM creditcard_analytics.dim_travel_profiles_history
ORDER BY effective_date DESC;

COMMENT ON VIEW creditcard_analytics.vw_dimension_version_changes IS 
'Unified view of all SCD2 dimension changes across customers and travel profiles. Shows versioning activity.';

-- =====================================================================
--  Summary View: High-Risk Transactions
-- =====================================================================

CREATE OR REPLACE VIEW creditcard_analytics.vw_high_risk_transactions AS
SELECT 
    transaction_id,
    tx_datetime,
    tx_amount,
    customer_id,
    customer_home_city,
    customer_home_region,
    terminal_id,
    tx_terminal_city,
    tx_terminal_region,
    distance_from_home_km,
    -- Risk factors
    is_unusual_amount,
    is_cross_region,
    is_far_from_home,
    is_travel_pattern_mismatch,
    -- Risk score (count of triggered flags)
    (CASE WHEN is_unusual_amount THEN 1 ELSE 0 END +
     CASE WHEN is_cross_region THEN 1 ELSE 0 END +
     CASE WHEN is_far_from_home THEN 1 ELSE 0 END +
     CASE WHEN is_travel_pattern_mismatch THEN 1 ELSE 0 END) as risk_score,
    -- Deviations
    amount_deviation_from_mean,
    ROUND((tx_amount - customer_mean_amount) / NULLIF(customer_std_amount, 0), 2) as z_score
FROM creditcard_analytics.fact_transactions_enriched
WHERE 
    is_unusual_amount = TRUE OR
    is_cross_region = TRUE OR
    is_far_from_home = TRUE OR
    is_travel_pattern_mismatch = TRUE
ORDER BY tx_datetime DESC;

COMMENT ON VIEW creditcard_analytics.vw_high_risk_transactions IS 
'Transactions flagged by one or more fraud indicators. Pre-filtered for Metabase fraud dashboards.';

-- Grant permissions (adjust user as needed)
GRANT USAGE ON SCHEMA creditcard_analytics TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA creditcard_analytics TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA creditcard_analytics TO postgres;

-- Log completion
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'creditcard_analytics schema initialized';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Current Dimensions:';
    RAISE NOTICE '  - dim_customers_current (SCD2)';
    RAISE NOTICE '  - dim_terminals_current (static)';
    RAISE NOTICE '  - dim_travel_profiles_current (SCD2)';
    RAISE NOTICE '';
    RAISE NOTICE 'Historical Tables:';
    RAISE NOTICE '  - dim_customers_history';
    RAISE NOTICE '  - dim_travel_profiles_history';
    RAISE NOTICE '';
    RAISE NOTICE 'Fact Table:';
    RAISE NOTICE '  - fact_transactions_enriched';
    RAISE NOTICE '';
    RAISE NOTICE 'Analytical Views:';
    RAISE NOTICE '  - vw_daily_fraud_summary';
    RAISE NOTICE '  - vw_regional_risk_profile';
    RAISE NOTICE '  - vw_customer_travel_evolution';
    RAISE NOTICE '  - vw_dimension_version_changes';
    RAISE NOTICE '  - vw_high_risk_transactions';
    RAISE NOTICE '========================================';
END $$;