-- Data Drift Scenario 2: Introduce statistical outliers
-- This demonstrates GX detecting values outside expected ranges
-- Run with: docker-compose exec postgres psql -U postgres -d tpcc -f /scripts/data_drift/02_add_outliers.sql
-- Note: Values are adjusted to be valid per TPC-C schema but outside GX expected ranges

BEGIN;

-- Add high amounts to order_line (we expect max 100000, but schema allows max 9999.99)
-- ol_amount is numeric(6,2), so this won't trigger anomaly - skip this update
-- Instead set to max schema allows
UPDATE order_line
SET ol_amount = 9999.99
WHERE ctid IN (
    SELECT ctid FROM order_line
    ORDER BY RANDOM()
    LIMIT 30
);

-- Add high quantity values (above expected 100 max in our expectations)
UPDATE order_line
SET ol_quantity = 999
WHERE ctid IN (
    SELECT ctid FROM order_line
    ORDER BY RANDOM()
    LIMIT 15
);

-- Add high discount values (above expected 0.5 max in our expectations)
-- c_discount is numeric(4,4), valid range is 0.0000 to 0.9999
UPDATE customer
SET c_discount = 0.9000
WHERE c_id IN (
    SELECT c_id FROM customer
    ORDER BY RANDOM()
    LIMIT 25
);

-- Add extreme balance (outside expected -100000 to 100000 range)
-- c_balance is numeric(12,2), so this is valid
UPDATE customer
SET c_balance = -500000.00
WHERE c_id IN (
    SELECT c_id FROM customer
    ORDER BY RANDOM()
    LIMIT 10
);

COMMIT;

-- Show what was changed
SELECT 'Statistical Outliers Applied' as status;
SELECT COUNT(*) as high_amounts FROM order_line WHERE ol_amount > 50000;
SELECT COUNT(*) as high_qty FROM order_line WHERE ol_quantity > 100;
SELECT COUNT(*) as high_discount FROM customer WHERE c_discount > 0.5;
SELECT COUNT(*) as extreme_balance FROM customer WHERE c_balance < -100000;
