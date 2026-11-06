-- 1. Overall revenue summary
SELECT
    SUM(gross_paid_revenue) AS gross_revenue,
    SUM(refunds) AS total_refunds,
    SUM(cancelled_after_payment) AS cancelled_amount,
    SUM(net_revenue) AS net_revenue
FROM daily_revenue;


-- 2. Daily order count
SELECT
    event_date,
    orders_with_purchase_events
FROM daily_orders;


-- 3. Order status distribution
SELECT
    final_order_status,
    COUNT(*) AS total_orders
FROM order_facts
GROUP BY final_order_status
ORDER BY total_orders DESC;


-- 4. Revenue by product category
SELECT
    category,
    SUM(allocated_revenue) AS total_revenue
FROM revenue_by_category
GROUP BY category
ORDER BY total_revenue DESC;
-- Top 10 products by allocated revenue
SELECT
    product_id,
    allocated_revenue,
    purchase_line_gross
FROM products
ORDER BY allocated_revenue DESC
LIMIT 10;