CREATE TABLE IF NOT EXISTS order_facts (
    order_id VARCHAR(50) PRIMARY KEY,
    paid_amount NUMERIC(14,2),
    payment_timestamp TIMESTAMP,
    refund_amount NUMERIC(14,2),
    final_order_status VARCHAR(50),
    cancelled_after_payment_amount NUMERIC(14,2),
    net_revenue NUMERIC(14,2),
    payment_date DATE
);

CREATE TABLE IF NOT EXISTS order_line_revenue (
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    category VARCHAR(100),
    line_gross NUMERIC(14,2),
    order_line_gross NUMERIC(14,2),
    paid_amount NUMERIC(14,2),
    payment_date DATE,
    net_revenue NUMERIC(14,2),
    allocated_revenue NUMERIC(14,2),
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE IF NOT EXISTS daily_revenue (
    payment_date DATE PRIMARY KEY,
    gross_paid_revenue NUMERIC(14,2),
    refunds NUMERIC(14,2),
    cancelled_after_payment NUMERIC(14,2),
    net_revenue NUMERIC(14,2),
    paid_orders INTEGER
);

CREATE TABLE IF NOT EXISTS daily_orders (
    event_date DATE PRIMARY KEY,
    orders_with_purchase_events INTEGER
);

CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(50) PRIMARY KEY,
    allocated_revenue NUMERIC(14,2),
    purchase_line_gross NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS product_daily (
    payment_date DATE,
    product_id VARCHAR(50),
    allocated_revenue NUMERIC(14,2),
    PRIMARY KEY (payment_date, product_id)
);

CREATE TABLE IF NOT EXISTS revenue_by_category (
    category VARCHAR(100) PRIMARY KEY,
    allocated_revenue NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS session_facts (
    session_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    event_count INTEGER,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    distinct_event_types INTEGER
);

CREATE TABLE IF NOT EXISTS customer_behaviour (
    customer_id VARCHAR(50) PRIMARY KEY,
    total_events INTEGER,
    sessions INTEGER,
    orders_referenced INTEGER
);

CREATE TABLE IF NOT EXISTS funnel_daily (
    event_date DATE PRIMARY KEY,
    search_sessions INTEGER,
    view_sessions INTEGER,
    cart_sessions INTEGER,
    purchase_sessions INTEGER
);

CREATE TABLE IF NOT EXISTS revenue_allocation_check (
    order_id VARCHAR(50) PRIMARY KEY,
    allocated_total NUMERIC(14,2),
    net_revenue NUMERIC(14,2),
    allocation_difference NUMERIC(14,2)
);