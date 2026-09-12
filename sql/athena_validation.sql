-- ============================================================
-- EUROSTAR AWS DATA PLATFORM
-- ATHENA DATA VALIDATION QUERIES
-- ============================================================

-- Purpose:
-- These queries were used to validate data across the
-- Bronze, Silver and Gold layers of the Eurostar data platform.


-- ============================================================
-- 1. BRONZE LAYER VALIDATION
-- ============================================================

-- Check journey row count
SELECT COUNT(*) AS journey_count
FROM railway_bronze_db.journeys;


-- Check for duplicate journey IDs
SELECT
    col0 AS journey_id,
    COUNT(*) AS duplicate_count
FROM railway_bronze_db.journeys
GROUP BY col0
HAVING COUNT(*) > 1;


-- Check for missing journey IDs
SELECT COUNT(*) AS null_journey_ids
FROM railway_bronze_db.journeys
WHERE col0 IS NULL;


-- Check distinct journey status values
SELECT DISTINCT col8 AS journey_status
FROM railway_bronze_db.journeys;


-- Check delay value range
SELECT
    MIN(CAST(col9 AS INTEGER)) AS min_delay,
    MAX(CAST(col9 AS INTEGER)) AS max_delay,
    AVG(CAST(col9 AS DOUBLE)) AS avg_delay
FROM railway_bronze_db.journeys;


-- Check journey status distribution
SELECT
    col8 AS journey_status,
    COUNT(*) AS journey_count
FROM railway_bronze_db.journeys
GROUP BY col8
ORDER BY journey_count DESC;


-- ============================================================
-- 2. SILVER LAYER VALIDATION
-- ============================================================

-- Check cleaned journey row count
SELECT COUNT(*) AS silver_journey_count
FROM eurostar_silver_db.journeys;


-- Check that journey IDs are unique
SELECT
    journey_id,
    COUNT(*) AS duplicate_count
FROM eurostar_silver_db.journeys
GROUP BY journey_id
HAVING COUNT(*) > 1;


-- Check for null primary keys
SELECT COUNT(*) AS null_journey_ids
FROM eurostar_silver_db.journeys
WHERE journey_id IS NULL;


-- Check cleaned journey statuses
SELECT
    journey_status,
    COUNT(*) AS journey_count
FROM eurostar_silver_db.journeys
GROUP BY journey_status
ORDER BY journey_count DESC;


-- Check delay statistics
SELECT
    MIN(delay_minutes) AS min_delay,
    MAX(delay_minutes) AS max_delay,
    AVG(delay_minutes) AS avg_delay
FROM eurostar_silver_db.journeys;


-- Validate booking relationship to journey
SELECT COUNT(*) AS orphan_bookings
FROM eurostar_silver_db.bookings b
LEFT JOIN eurostar_silver_db.journeys j
    ON b.journey_id = j.journey_id
WHERE j.journey_id IS NULL;


-- Validate payment relationship to booking
SELECT COUNT(*) AS orphan_payments
FROM eurostar_silver_db.payments p
LEFT JOIN eurostar_silver_db.bookings b
    ON p.booking_id = b.booking_id
WHERE b.booking_id IS NULL;


-- ============================================================
-- 3. GOLD LAYER VALIDATION
-- ============================================================

-- Validate fact_journeys row count
SELECT COUNT(*) AS fact_journey_count
FROM eurostar_gold_db.fact_journeys;


-- Check journey status distribution
SELECT
    journey_status,
    COUNT(*) AS journey_count
FROM eurostar_gold_db.fact_journeys
GROUP BY journey_status
ORDER BY journey_count DESC;


-- Validate on-time journey count
SELECT
    SUM(is_on_time) AS on_time_journeys
FROM eurostar_gold_db.fact_journeys;


-- Validate delayed journey count
SELECT
    SUM(is_delayed) AS delayed_journeys
FROM eurostar_gold_db.fact_journeys;


-- Validate cancelled journey count
SELECT
    SUM(is_cancelled) AS cancelled_journeys
FROM eurostar_gold_db.fact_journeys;


-- Check total bookings
SELECT COUNT(*) AS total_bookings
FROM eurostar_gold_db.fact_bookings;


-- Check total payment amount
SELECT SUM(amount) AS total_payment_amount
FROM eurostar_gold_db.fact_payments;


-- Check total refund amount
SELECT SUM(refund_amount) AS total_refund_amount
FROM eurostar_gold_db.fact_refunds;


-- Check total disruptions
SELECT COUNT(*) AS total_disruptions
FROM eurostar_gold_db.fact_disruptions;


-- Check disruption severity distribution
SELECT
    severity,
    COUNT(*) AS disruption_count
FROM eurostar_gold_db.fact_disruptions
GROUP BY severity
ORDER BY disruption_count DESC;


-- Check disruption resolution status
SELECT
    resolution_status,
    COUNT(*) AS disruption_count
FROM eurostar_gold_db.fact_disruptions
GROUP BY resolution_status
ORDER BY disruption_count DESC;


-- ============================================================
-- END OF ATHENA VALIDATION QUERIES
-- ============================================================
