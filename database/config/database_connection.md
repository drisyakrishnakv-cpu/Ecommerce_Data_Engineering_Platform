# PostgreSQL Database Connection

## Project
Real-Time E-Commerce Data Engineering Platform

## Database Details

- Host: `localhost`
- Port: `5432`
- Database: `ecommerce_analytics`
- Username: `ecommerce_user`
- Password: Obtain from the project `.env` file
- Database type: PostgreSQL 16

## Available Analytical Tables

1. order_facts
2. order_line_revenue
3. daily_revenue
4. daily_orders
5. products
6. product_daily
7. revenue_by_category
8. session_facts
9. customer_behaviour
10. funnel_daily
11. revenue_allocation_check

## Python Connection

Use SQLAlchemy with psycopg2 to connect Streamlit to PostgreSQL.

The database credentials are stored in the `.env` file.

**Important:** When the dashboard runs on the same computer, `localhost` can be used. If Streamlit runs in another Docker container or on another computer, the host must be changed to the appropriate service name or reachable IP address.
