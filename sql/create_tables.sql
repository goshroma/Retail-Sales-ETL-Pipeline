-- create_tables.sql
-- ---------------------------------------------------------------------------
-- Schema for the processed retail sales table that src/load_to_mysql.py
-- writes into.
--
-- Note: Spark's JDBC writer (mode="overwrite") is capable of creating this
-- table itself on first run, inferring SQL types from the DataFrame's
-- schema. Running this script by hand first is optional, but it documents
-- the intended schema explicitly and lets you pick deliberate SQL types
-- (e.g. DECIMAL for money instead of whatever Spark happens to infer) and
-- indexes before the first pipeline run.
-- ---------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS retail_sales_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE retail_sales_db;

DROP TABLE IF EXISTS retail_sales_processed;

CREATE TABLE retail_sales_processed (
    transaction_id    VARCHAR(20)    NOT NULL,
    customer_id       VARCHAR(20)    NOT NULL,
    product_id        VARCHAR(20)    NOT NULL,
    category          VARCHAR(50)    NOT NULL,
    city              VARCHAR(50)    NOT NULL,
    state             VARCHAR(50)    NOT NULL,
    quantity          INT            NOT NULL,
    unit_price        DECIMAL(10, 2) NOT NULL,
    discount          DECIMAL(5, 4)  NOT NULL,
    total_amount      DECIMAL(12, 2) NOT NULL,
    payment_method    VARCHAR(30)    NOT NULL,
    order_date        DATE           NOT NULL,
    delivery_date     DATE           NOT NULL,
    revenue           DECIMAL(12, 2) NOT NULL,
    profit            DECIMAL(12, 2) NOT NULL,
    delivery_days     INT,
    order_month       INT,
    order_year        INT,
    order_year_month  VARCHAR(7),
    PRIMARY KEY (transaction_id)
);

CREATE INDEX idx_order_date    ON retail_sales_processed (order_date);
CREATE INDEX idx_category      ON retail_sales_processed (category);
CREATE INDEX idx_state         ON retail_sales_processed (state);
CREATE INDEX idx_customer_id   ON retail_sales_processed (customer_id);
