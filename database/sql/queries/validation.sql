-- Check revenue allocation differences
SELECT
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (
        WHERE ABS(allocation_difference) > 0.01
    ) AS mismatched_orders,
    SUM(ABS(allocation_difference)) AS total_absolute_difference
FROM revenue_allocation_check;


-- Display orders with allocation mismatches
SELECT *
FROM revenue_allocation_check
WHERE ABS(allocation_difference) > 0.01;
-- Daily conversion funnel
SELECT
    event_date,
    search_sessions,
    view_sessions,
    cart_sessions,
    purchase_sessions,

    ROUND(
        purchase_sessions * 100.0 /
        NULLIF(search_sessions, 0), 2
    ) AS search_to_purchase_rate

FROM funnel_daily;