-- STEP 1: Explore raw data
select * from grocery_chain_data;

-- STEP 2: Create the RAW data table (ingested directly from source system)
CREATE TABLE grocery_raw (
    customer_id INT, store_name VARCHAR(100), transaction_date VARCHAR(20),
    aisle VARCHAR(100), product_name VARCHAR(100), quantity INT, unit_price DECIMAL(10 , 2 ),
    total_amount DECIMAL(10 , 2 ), discount_amount DECIMAL(10 , 2 ), final_amount DECIMAL(10 , 2 ),
    loyalty_points INT
);

-- STEP 3: Inspect raw data
select * from grocery_raw;

-- STEP 4: Create STAGING table (to perform cleaning & transformation)
CREATE TABLE grocery_staging AS
SELECT * FROM grocery_raw;

-- STEP 5: Add surrogate primary key for tracking records
ALTER TABLE grocery_staging
ADD COLUMN staging_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY;
    
-- STEP 6: Remove duplicates (keep most recent record based on transaction_date & staging_id)
WITH duplicates AS (
    SELECT 
	  staging_id,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, store_name, transaction_date, product_name, quantity
            ORDER BY transaction_date DESC, staging_id DESC
        ) AS rn
    FROM grocery_staging )
DELETE s
FROM grocery_staging s
JOIN duplicates d ON s.staging_id = d.staging_id
WHERE d.rn > 1;

-- STEP 7: Standardize transaction_date format (DD-MM-YYYY → DATE)
UPDATE grocery_staging
SET transaction_date = STR_TO_DATE(transaction_date, '%d-%m-%Y');

-- STEP 8: Handle missing text values (replace NULL/blank with 'UNKNOWN')
UPDATE grocery_staging
SET store_name   = CASE WHEN store_name IS NULL OR store_name = '' THEN 'UNKNOWN' ELSE store_name END,
    aisle        = CASE WHEN aisle IS NULL OR aisle = '' THEN 'UNKNOWN' ELSE aisle END,
    product_name = CASE WHEN product_name IS NULL OR product_name = '' THEN 'UNKNOWN' ELSE product_name END;
    
-- STEP 9: Handle missing numeric values (set NULL → 0 for quantity & discount)
UPDATE grocery_staging
SET quantity = COALESCE(quantity, 0),
    discount_amount = COALESCE(discount_amount, 0);

-- STEP 10: Flag invalid pricing records (unit_price missing or zero)
ALTER TABLE grocery_staging ADD COLUMN data_issue_flag TINYINT DEFAULT 0;

UPDATE grocery_staging
SET data_issue_flag = 1
WHERE unit_price IS NULL OR unit_price = 0;

-- STEP 11: Validate discounts and recalculate final amounts
ALTER TABLE grocery_staging ADD COLUMN discount_issue_flag TINYINT DEFAULT 0;

UPDATE grocery_staging
SET 
    discount_issue_flag = CASE 
        WHEN discount_amount > total_amount THEN 1 ELSE 0 END,
    total_amount = COALESCE(quantity, 0) * COALESCE(unit_price, 0),
    discount_amount = LEAST(COALESCE(discount_amount, 0), COALESCE(total_amount, 0)),
    final_amount = GREATEST(COALESCE(total_amount, 0) - COALESCE(discount_amount, 0), 0);
    
-- STEP 12: Adjust loyalty points (no negative points allowed)
UPDATE grocery_staging
SET loyalty_points = FLOOR(final_amount / 10)
WHERE loyalty_points < 0;

-- STEP 13: Create CLEANED table (final dataset for analysis)
CREATE TABLE grocery_cleaned AS
SELECT DISTINCT
    customer_id, store_name,
    STR_TO_DATE(transaction_date, '%Y-%m-%d') AS transaction_date, 
    aisle, product_name, quantity, unit_price, total_amount, discount_amount, final_amount, loyalty_points      
FROM grocery_staging;

select * from grocery_cleaned;

-- STEP 14: Create indexes to optimize analytical queries
CREATE INDEX idx_cleaned_transaction_date ON grocery_cleaned (transaction_date);
CREATE INDEX idx_cleaned_customer ON grocery_cleaned (customer_id);
CREATE INDEX idx_cleaned_store ON grocery_cleaned (store_name);
CREATE INDEX idx_cleaned_product ON grocery_cleaned (product_name);

-- STEP 15: Data Quality Check - Record Count
SELECT COUNT(*) AS total_records FROM grocery_cleaned;

-- STEP 16: Data Quality Check - Missing Values
SELECT 
    SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) AS null_quantity,
    SUM(CASE WHEN unit_price IS NULL THEN 1 ELSE 0 END) AS null_unit_price,
    SUM(CASE WHEN discount_amount IS NULL THEN 1 ELSE 0 END) AS null_discount
FROM grocery_cleaned;

-- STEP 17: Transaction Date Range & Active Days
SELECT MIN(transaction_date) AS start_date, MAX(transaction_date) AS end_date,
       COUNT(DISTINCT transaction_date) AS active_days
FROM grocery_cleaned;

-- Sales Overview (Gross, Discounts, Net Sales)
SELECT 
    SUM(total_amount) AS gross_sales, SUM(discount_amount) AS total_discounts, 
    SUM(final_amount) AS net_sales
FROM grocery_cleaned;

-- Sales by Store
SELECT store_name, SUM(final_amount) AS sales FROM grocery_cleaned
GROUP BY store_name ORDER BY sales DESC;

-- Sales by Product
SELECT product_name, SUM(final_amount) AS sales FROM grocery_cleaned
GROUP BY product_name ORDER BY sales DESC LIMIT 10;

-- Repeat vs. One-time Customers
SELECT 
    CASE WHEN order_count > 1 THEN 'Repeat' ELSE 'One-time' END AS customer_type, COUNT(*) AS num_customers
FROM ( SELECT customer_id, COUNT(*) AS order_count FROM grocery_cleaned
         GROUP BY customer_id ) t
GROUP BY customer_type;

-- Customer Lifetime Value (LTV)
SELECT customer_id, SUM(final_amount) AS lifetime_value FROM grocery_cleaned
GROUP BY customer_id ORDER BY lifetime_value DESC LIMIT 50;

-- Impact of Discounts on Sales
SELECT 
    CASE 
        WHEN discount_amount = 0 THEN 'No Discount'
        WHEN discount_amount BETWEEN 1 AND 10 THEN 'Low Discount'
        WHEN discount_amount BETWEEN 11 AND 50 THEN 'Medium Discount'
        ELSE 'High Discount'
    END AS discount_bucket, COUNT(*) AS num_transactions, SUM(final_amount) AS total_sales
FROM grocery_cleaned
GROUP BY discount_bucket ORDER BY total_sales DESC;

-- Daily Sales Trend
SELECT transaction_date, SUM(final_amount) AS daily_sales FROM grocery_cleaned
GROUP BY transaction_date ORDER BY transaction_date;

-- Monthly Sales Trend
SELECT DATE_FORMAT(transaction_date, '%Y-%m') AS month, SUM(final_amount) AS monthly_sales
FROM grocery_cleaned
GROUP BY DATE_FORMAT(transaction_date, '%Y-%m') ORDER BY month;

-- Day of Week Shopping Pattern
SELECT DAYNAME(transaction_date) AS day_of_week, SUM(final_amount) AS sales FROM grocery_cleaned
GROUP BY day_of_week ORDER BY sales DESC;

-- Basket Size Analysis (Average Items per Order)
SELECT customer_id, COUNT(DISTINCT product_name) AS unique_products, SUM(quantity) AS total_items
FROM grocery_cleaned
GROUP BY customer_id ORDER BY total_items DESC LIMIT 10;

-- Loyalty Points Analysis (Engagement Metric)
SELECT customer_id, SUM(loyalty_points) AS total_points FROM grocery_cleaned
GROUP BY customer_id ORDER BY total_points DESC LIMIT 10;