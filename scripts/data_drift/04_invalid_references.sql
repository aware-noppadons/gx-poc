-- Data Drift Scenario 4: Break referential integrity
-- This demonstrates GX detecting foreign key violations and orphan records
-- Run with: docker-compose exec postgres psql -U postgres -d tpcc -f /scripts/data_drift/04_invalid_references.sql

BEGIN;

-- First, get a valid warehouse and district for our fake orders
DO $$
DECLARE
    v_w_id INTEGER;
    v_d_id INTEGER;
    v_max_order_id INTEGER;
BEGIN
    -- Get first warehouse
    SELECT w_id INTO v_w_id FROM warehouse LIMIT 1;

    -- Get first district
    SELECT d_id INTO v_d_id FROM district WHERE d_w_id = v_w_id LIMIT 1;

    -- Get max order id to avoid conflicts
    SELECT COALESCE(MAX(o_id), 0) INTO v_max_order_id FROM orders;

    -- Create orders referencing non-existent customers
    INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
    VALUES
        (v_max_order_id + 99901, v_d_id, v_w_id, 999901, NOW(), 1, 5, 1),
        (v_max_order_id + 99902, v_d_id, v_w_id, 999902, NOW(), 1, 3, 1),
        (v_max_order_id + 99903, v_d_id, v_w_id, 999903, NOW(), 1, 4, 1);

    -- Create order_lines referencing non-existent items
    INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info)
    VALUES
        (v_max_order_id + 99901, v_d_id, v_w_id, 1, 999901, v_w_id, NOW(), 5, 100.00, 'ORPHAN ITEM REF'),
        (v_max_order_id + 99902, v_d_id, v_w_id, 1, 999902, v_w_id, NOW(), 3, 75.00, 'ORPHAN ITEM REF'),
        (v_max_order_id + 99903, v_d_id, v_w_id, 1, 999903, v_w_id, NOW(), 4, 80.00, 'ORPHAN ITEM REF');

    RAISE NOTICE 'Created orphan orders and order_lines in warehouse % district %', v_w_id, v_d_id;
END $$;

-- Log the drift
INSERT INTO drift_log (drift_type, description)
VALUES ('referential_integrity', 'Created 3 orders with invalid customer IDs and 3 order_lines with invalid item IDs');

COMMIT;

-- Show what was changed
SELECT 'Referential Integrity Violations Applied' as status;

-- Show orders with invalid customer references
SELECT 'Orders with invalid customers:' as check_type;
SELECT o_id, o_c_id, o_entry_d
FROM orders
WHERE o_c_id > 900000;

-- Show order_lines with invalid item references
SELECT 'Order lines with invalid items:' as check_type;
SELECT ol_o_id, ol_i_id, ol_dist_info
FROM order_line
WHERE ol_i_id > 900000;
