-- ============================================================
-- EUROSTAR AWS DATA PLATFORM
-- AMAZON REDSHIFT / SPECTRUM QUERIES
-- ============================================================

-- Purpose:
-- These queries were used to explore and validate the
-- Gold analytical layer through Amazon Redshift Spectrum.


-- ============================================================
-- 1. CHECK GOLD TABLES
-- ============================================================

SELECT *
FROM eurostar_gold.fact_journeys
LIMIT 10;


SELECT *
FROM eurostar_gold.fact_bookings
LIMIT 10;


SELECT *
FROM eurostar_gold.fact_disruptions
LIMIT 10;


-- ============================================================
-- 2. JOURNEY PERFORMANCE
-- ============================================================

-- Total journeys
SELECT
    COUNT(*) AS total_journeys
FROM eurostar_gold.fact_journeys;


-- Journey status breakdown
SELECT
    journey_status,
    COUNT(*) AS journey_count
FROM eurostar_gold.fact_journeys
GROUP BY journey_status
ORDER BY journey_count DESC;


-- On-time performance
SELECT
    SUM(is_on_time) AS on_time_journeys,
    SUM(is_delayed) AS delayed_journeys,
    SUM(is_cancelled) AS cancelled_journeys
FROM eurostar_gold.fact_journeys;


-- Average delay
SELECT
    AVG(delay_minutes) AS average_delay_minutes
FROM eurostar_gold.fact_journeys;


-- ============================================================
-- 3. JOURNEYS BY DESTINATION
-- ============================================================

SELECT
    s.station_name AS destination_station,
    COUNT(*) AS journey_count
FROM eurostar_gold.fact_journeys j
JOIN eurostar_gold.dim_station s
    ON j.destination_station_id = s.station_id
GROUP BY s.station_name
ORDER BY journey_count DESC;


-- ============================================================
-- 4. JOURNEYS OVER TIME
-- ============================================================

SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(*) AS journey_count
FROM eurostar_gold.fact_journeys j
JOIN eurostar_gold.dim_date d
    ON j.journey_date_key = d.date_key
GROUP BY
    d.year,
    d.month,
    d.month_name
ORDER BY
    d.year,
    d.month;


-- ============================================================
-- 5. AVERAGE DELAY BY MONTH
-- ============================================================

SELECT
    d.year,
    d.month,
    d.month_name,
    AVG(j.delay_minutes) AS average_delay_minutes
FROM eurostar_gold.fact_journeys j
JOIN eurostar_gold.dim_date d
    ON j.journey_date_key = d.date_key
GROUP BY
    d.year,
    d.month,
    d.month_name
ORDER BY
    d.year,
    d.month;


-- ============================================================
-- 6. BOOKING ANALYSIS
-- ============================================================

SELECT
    COUNT(*) AS total_bookings,
    SUM(ticket_value) AS total_ticket_value,
    AVG(ticket_price) AS average_ticket_price,
    SUM(passenger_count) AS total_passengers
FROM eurostar_gold.fact_bookings;


-- Booking channel distribution
SELECT
    booking_channel,
    COUNT(*) AS booking_count
FROM eurostar_gold.fact_bookings
GROUP BY booking_channel
ORDER BY booking_count DESC;


-- ============================================================
-- 7. PAYMENT ANALYSIS
-- ============================================================

SELECT
    payment_status,
    COUNT(*) AS payment_count,
    SUM(amount) AS total_amount
FROM eurostar_gold.fact_payments
GROUP BY payment_status
ORDER BY payment_count DESC;


-- ============================================================
-- 8. REFUND ANALYSIS
-- ============================================================

SELECT
    refund_status,
    COUNT(*) AS refund_count,
    SUM(refund_amount) AS total_refund_amount
FROM eurostar_gold.fact_refunds
GROUP BY refund_status
ORDER BY refund_count DESC;


-- ============================================================
-- 9. DISRUPTION ANALYSIS
-- ============================================================

-- Total disruptions
SELECT
    COUNT(*) AS total_disruptions
FROM eurostar_gold.fact_disruptions;


-- Disruptions by reason
SELECT
    disruption_reason,
    COUNT(*) AS disruption_count
FROM eurostar_gold.fact_disruptions
GROUP BY disruption_reason
ORDER BY disruption_count DESC;


-- Disruption severity
SELECT
    severity,
    COUNT(*) AS disruption_count
FROM eurostar_gold.fact_disruptions
GROUP BY severity
ORDER BY disruption_count DESC;


-- Resolution status
SELECT
    resolution_status,
    COUNT(*) AS disruption_count
FROM eurostar_gold.fact_disruptions
GROUP BY resolution_status
ORDER BY disruption_count DESC;


-- Average delay by disruption reason
SELECT
    disruption_reason,
    AVG(delay_minutes) AS average_delay_minutes
FROM eurostar_gold.fact_disruptions
GROUP BY disruption_reason
ORDER BY average_delay_minutes DESC;


-- ============================================================
-- END OF REDSHIFT QUERIES
-- ============================================================
