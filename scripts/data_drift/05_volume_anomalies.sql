-- Data Drift Scenario 5: Abnormal Volume Changes
-- This demonstrates GX detecting sudden spikes or drops in data volume
-- Run with: docker-compose exec postgres psql -U postgres -d tpcc -f /scripts/data_drift/05_volume_anomalies.sql

-- ============================================================
-- SCENARIO 5A: Volume Spike - Mass insert of suspicious orders
-- ============================================================

BEGIN;

DO $$
DECLARE
    v_w_id INTEGER;
    v_d_id INTEGER;
    v_c_id INTEGER;
    v_max_order_id INTEGER;
    v_i INTEGER;
BEGIN
    -- Get valid references
    SELECT w_id INTO v_w_id FROM warehouse LIMIT 1;
    SELECT d_id INTO v_d_id FROM district WHERE d_w_id = v_w_id LIMIT 1;
    SELECT c_id INTO v_c_id FROM customer WHERE c_w_id = v_w_id AND c_d_id = v_d_id LIMIT 1;
    SELECT COALESCE(MAX(o_id), 0) INTO v_max_order_id FROM orders;

    -- Insert 500 orders in a single batch (abnormal spike)
    FOR v_i IN 1..500 LOOP
        INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
        VALUES (
            v_max_order_id + 800000 + v_i,
            v_d_id,
            v_w_id,
            v_c_id,
            NOW(),
            (v_i % 10) + 1,
            (v_i % 10) + 1,
            1
        );
    END LOOP;

    RAISE NOTICE 'Inserted 500 spike orders starting from ID %', v_max_order_id + 800001;
END $$;

-- Log the drift
INSERT INTO drift_log (drift_type, description)
VALUES ('volume_spike', 'Inserted 500 orders in a single batch - simulating abnormal order volume spike');

COMMIT;

-- ============================================================
-- SCENARIO 5B: Volume Drop - Mass delete simulation
-- (We mark records instead of deleting to allow reversal)
-- ============================================================

BEGIN;

-- Create a backup table if it doesn't exist for "soft delete" simulation
CREATE TABLE IF NOT EXISTS history_backup AS
SELECT * FROM history WHERE 1=0;

-- Move 30% of history records to backup (simulating data loss/purge)
WITH moved_records AS (
    DELETE FROM history
    WHERE ctid IN (
        SELECT ctid FROM history
        ORDER BY RANDOM()
        LIMIT (SELECT COUNT(*) * 0.3 FROM history)::INTEGER
    )
    RETURNING *
)
INSERT INTO history_backup
SELECT * FROM moved_records;

-- Log the drift
INSERT INTO drift_log (drift_type, description)
VALUES ('volume_drop', 'Removed 30% of history records - simulating abnormal data purge/loss');

COMMIT;

-- ============================================================
-- SCENARIO 5C: Suspicious pattern - All orders at same time
-- ============================================================

BEGIN;

-- Update a batch of orders to have identical timestamps (bot-like behavior)
UPDATE orders
SET o_entry_d = '2024-01-15 03:00:00'::TIMESTAMP
WHERE o_id IN (
    SELECT o_id FROM orders
    WHERE o_id < 800000  -- Only update original orders
    ORDER BY RANDOM()
    LIMIT 200
);

-- Log the drift
INSERT INTO drift_log (drift_type, description)
VALUES ('suspicious_pattern', 'Set 200 orders to identical timestamp - simulating bot/fraud pattern');

COMMIT;

-- ============================================================
-- Summary Report
-- ============================================================

SELECT '=== Volume Anomalies Applied ===' as status;

SELECT 'Order Volume Check:' as metric;
SELECT
    COUNT(*) as total_orders,
    COUNT(*) FILTER (WHERE o_id >= 800000) as spike_orders,
    ROUND(COUNT(*) FILTER (WHERE o_id >= 800000)::NUMERIC / COUNT(*) * 100, 2) as spike_percentage
FROM orders;

SELECT 'History Volume Check:' as metric;
SELECT
    (SELECT COUNT(*) FROM history) as current_history,
    (SELECT COUNT(*) FROM history_backup) as removed_history;

SELECT 'Timestamp Pattern Check:' as metric;
SELECT
    o_entry_d,
    COUNT(*) as order_count
FROM orders
GROUP BY o_entry_d
HAVING COUNT(*) > 50
ORDER BY order_count DESC
LIMIT 5;

SELECT 'Drift Log:' as metric;
SELECT drift_type, description, created_at
FROM drift_log
WHERE drift_type IN ('volume_spike', 'volume_drop', 'suspicious_pattern')
ORDER BY created_at DESC;
