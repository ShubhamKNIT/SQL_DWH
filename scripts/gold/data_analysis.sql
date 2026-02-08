-- ============================================================
-- Dimension: Customers
-- ============================================================

-- Checking duplicates after join
SELECT 
    cst_id,
    COUNT(*)
FROM (
    SELECT
        ci.cst_id,
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_marital_status,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
    FROM silver.crm_cust_info AS ci
    LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid
) AS t
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- Multiple columns for gender
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen,
    CASE 
        WHEN ci.cst_gndr != 'n/a'
            THEN ci.cst_gndr
        WHEN ca.gen != 'n/a'
            THEN ca.gen
        ELSE
            'n/a'
    END AS new_gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
ORDER BY 1, 2;

-- Rules for merging gender
-- CRM is master table => so use the value from it
-- If data is not available in CRM use it from ERP
-- If the both table contains NULL so use n/a

DROP VIEW IF EXISTS gold.dim_customers;
CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a'
            THEN ci.cst_gndr
        WHEN ca.gen != 'n/a'
            THEN ca.gen
        ELSE
            'n/a'
    END AS gender,
    ca.bdate AS birth_date,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
ON ci.cst_key = la.cid;

-- ============================================================
-- Dimension: Products
-- ============================================================

-- Checking duplicates after join
SELECT 
    prd_key,
    COUNT(*)
FROM (
    SELECT
        pi.prd_id,
        pi.cat_id,
        pi.prd_key,
        pi.prd_nm,
        pi.prd_cost,
        pi.prd_line,
        pi.prd_start_dt,
        -- pi.prd_end_dt, -- no need of prd_end_dt
        pc.id,
        pc.cat,
        pc.subcat,
        pc.maintenance
    FROM silver.crm_prd_info AS pi
    LEFT JOIN silver.erp_px_cat_g1v2 AS pc
    ON pi.cat_id = pc.id
    WHERE pi.prd_end_dt IS NULL -- filter out historical data
) AS t
GROUP BY prd_key
HAVING COUNT(*) > 1;

DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER(ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key,
    pi.prd_id AS product_id,
    pi.prd_key AS product_number,
    pi.prd_nm AS product_name,
    pi.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pi.prd_cost AS cost,
    pi.prd_line AS product_line,
    pi.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pi
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pi.cat_id = pc.id
WHERE pi.prd_end_dt IS NULL; -- filter out historical data

-- ============================================================
-- Fact: Sales
-- ============================================================
DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    dp.product_key AS product_key,
    dc.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS dp
ON sd.sls_prd_key = dp.product_number
LEFT JOIN gold.dim_customers AS dc
ON sd.sls_cust_id = dc.customer_id;

SELECT *
FROM gold.fact_sales AS fs
LEFT JOIN gold.dim_customers AS dc
ON fs.customer_key = dc.customer_key
LEFT JOIN gold.dim_products AS dp
ON fs.product_key = dp.product_key
-- WHERE dc.customer_key IS NULL;
WHERE dp.product_key IS NULL;