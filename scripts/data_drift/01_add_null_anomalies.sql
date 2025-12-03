-- Data Drift Scenario 1: Introduce NULL values in columns that allow NULLs
-- This demonstrates GX detecting unexpected null values
-- Run with: docker-compose exec postgres psql -U postgres -d tpcc -f /scripts/data_drift/01_add_null_anomalies.sql
-- Note: TPC-C schema has many NOT NULL constraints; using columns that allow NULLs

BEGIN;

-- Add NULLs to order_line delivery date (ol_delivery_d allows NULL)
UPDATE order_line
SET ol_delivery_d = NULL
WHERE ctid IN (
    SELECT ctid FROM order_line
    WHERE ol_delivery_d IS NOT NULL
    ORDER BY RANDOM()
    LIMIT 500
);

-- Add NULLs to order_line amount (ol_amount allows NULL)
UPDATE order_line
SET ol_amount = NULL
WHERE ctid IN (
    SELECT ctid FROM order_line
    WHERE ol_amount IS NOT NULL
    ORDER BY RANDOM()
    LIMIT 100
);

-- Add NULLs to order_line dist_info (ol_dist_info allows NULL)
UPDATE order_line
SET ol_dist_info = NULL
WHERE ctid IN (
    SELECT ctid FROM order_line
    WHERE ol_dist_info IS NOT NULL
    ORDER BY RANDOM()
    LIMIT 200
);

COMMIT;

-- Show what was changed
SELECT 'NULL Anomalies Applied' as status;
SELECT COUNT(*) as null_delivery_dates FROM order_line WHERE ol_delivery_d IS NULL;
SELECT COUNT(*) as null_amounts FROM order_line WHERE ol_amount IS NULL;
SELECT COUNT(*) as null_dist_info FROM order_line WHERE ol_dist_info IS NULL;
