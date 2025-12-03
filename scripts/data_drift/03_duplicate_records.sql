-- Data Drift Scenario 3: Create duplicate records
-- This demonstrates GX detecting uniqueness violations and semantic duplicates
-- Run with: docker-compose exec postgres psql -U postgres -d tpcc -f /scripts/data_drift/03_duplicate_records.sql

BEGIN;

-- Create duplicate item entries with same name (semantic duplicates)
-- These have different IDs but same product name = data quality issue
INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data)
SELECT
    i_id + 100000,          -- New ID to avoid PK violation
    i_im_id,
    i_name,                 -- Same name = semantic duplicate
    i_price * 1.1,          -- Slightly different price
    i_data || ' DUPLICATE'
FROM item
WHERE i_id <= 50;

-- Create items with suspiciously similar names (near-duplicates)
INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data)
SELECT
    i_id + 200000,
    i_im_id,
    i_name || ' v2',        -- Slight variation
    i_price,
    i_data
FROM item
WHERE i_id BETWEEN 51 AND 75;

-- Log the drift
INSERT INTO drift_log (drift_type, description)
VALUES ('duplicate_records', 'Created 50 semantic duplicate items and 25 near-duplicate items');

COMMIT;

-- Show what was changed
SELECT 'Duplicate Records Applied' as status;
SELECT COUNT(*) as total_items FROM item;
SELECT COUNT(*) as duplicate_items FROM item WHERE i_id >= 100000;
SELECT i_name, COUNT(*) as count
FROM item
GROUP BY i_name
HAVING COUNT(*) > 1
ORDER BY count DESC
LIMIT 10;
