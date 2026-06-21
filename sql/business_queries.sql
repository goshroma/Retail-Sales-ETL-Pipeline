-- business_queries.sql
-- ---------------------------------------------------------------------------
-- Five core business questions, run directly against retail_sales_processed
-- in MySQL (loaded by src/load_to_mysql.py). Each query is independent and
-- can be run on its own in a MySQL client or pasted into a BI tool's
-- "custom SQL" panel.
-- ---------------------------------------------------------------------------

USE retail_sales_db;

-- =============================================================================
-- 1. Monthly sales trend
-- =============================================================================
SELECT
    order_year,
    order_month,
    order_year_month,
    ROUND(SUM(revenue), 2)           AS total_revenue,
    COUNT(DISTINCT transaction_id)   AS total_orders
FROM retail_sales_processed
GROUP BY order_year, order_month, order_year_month
ORDER BY order_year, order_month;


-- =============================================================================
-- 2. Best performing category (by revenue)
-- =============================================================================
SELECT
    category,
    ROUND(SUM(revenue), 2)           AS total_revenue,
    ROUND(SUM(profit), 2)            AS total_profit,
    COUNT(DISTINCT transaction_id)   AS total_orders
FROM retail_sales_processed
GROUP BY category
ORDER BY total_revenue DESC
LIMIT 1;

-- Full ranking across every category, for context / charting:
SELECT
    category,
    ROUND(SUM(revenue), 2)  AS total_revenue,
    ROUND(SUM(profit), 2)   AS total_profit
FROM retail_sales_processed
GROUP BY category
ORDER BY total_revenue DESC;


-- =============================================================================
-- 3. Top 5 customers (by total revenue)
-- =============================================================================
SELECT
    customer_id,
    COUNT(DISTINCT transaction_id)   AS total_orders,
    ROUND(SUM(revenue), 2)           AS total_revenue,
    ROUND(AVG(revenue), 2)           AS avg_order_value
FROM retail_sales_processed
WHERE customer_id <> 'GUEST_CUSTOMER'  -- excluded: not a single identifiable customer
GROUP BY customer_id
ORDER BY total_revenue DESC
LIMIT 5;


-- =============================================================================
-- 4. Revenue by state
-- =============================================================================
SELECT
    state,
    ROUND(SUM(revenue), 2)           AS total_revenue,
    COUNT(DISTINCT transaction_id)   AS total_orders,
    COUNT(DISTINCT customer_id)      AS total_customers
FROM retail_sales_processed
GROUP BY state
ORDER BY total_revenue DESC;


-- =============================================================================
-- 5. Average delivery time (days)
-- =============================================================================
-- A small number of rows have delivery_date < order_date (a logged data
-- error, not a real delivery), so they're excluded here rather than allowed
-- to drag the average down with negative day counts.
SELECT
    ROUND(AVG(delivery_days), 2)   AS avg_delivery_days,
    MIN(delivery_days)             AS min_delivery_days,
    MAX(delivery_days)             AS max_delivery_days
FROM retail_sales_processed
WHERE delivery_days >= 0;
