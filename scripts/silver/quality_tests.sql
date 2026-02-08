-- Change schema silver => silver

-- ==========================================
-- TABLE: silver.crm_cust_info
-- ==========================================

-- Check for NULLs/Duplicates in Primary Key
-- Expectation: No result

SELECT
    cst_id,
    COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 
    OR cst_id IS NULL;

-- Unwanted Space
-- Expectation: No result

SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- Data Standardization and Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

-- ==========================================
-- TABLE: silver.crm_prd_info
-- ==========================================

-- Check for NULLs/Duplicates in Primary Key
-- Expectation: No result

SELECT
    prd_id,
    COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 
    OR prd_id IS NULL;

-- Unwanted Space
-- Expectation: No result

SELECT prd_key
FROM silver.crm_prd_info
WHERE prd_key != TRIM(prd_key);

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

SELECT prd_line
FROM silver.crm_prd_info
WHERE prd_line != TRIM(prd_line);

-- Negative cost or No cost
SELECT *
FROM silver.crm_prd_info
WHERE prd_cost < 0 
    OR prd_cost IS NULL;

SELECT 
    prd_id,
    COALESCE(prd_cost, 0) AS prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0
    OR prd_cost IS NULL;

-- Data Stanadardization and Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check start data and end date
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt IS NULL;

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt IS NULL;

-- ==========================================
-- TABLE: silver.crm_sales_details
-- ==========================================

-- Check presence of prd_key in TABLE - silver.crm_prd_info
-- Check presence of cust_id in TABLE - silver.crm_cust_info
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
-- WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Check dates attributes 
-- univariate analysis
SELECT sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= '0' OR LENGTH(sls_order_dt) != 8 OR sls_order_dt > '20500101';

SELECT sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt <= '0' OR LENGTH(sls_ship_dt) != 8 OR sls_ship_dt > '20500101';

SELECT sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= '0' OR LENGTH(sls_due_dt) != 8 OR sls_due_dt > '20500101';

-- bivariate analysis (NEED TO BE SOLVE)
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
    OR sls_order_dt > sls_due_dt
    OR sls_ship_dt > sls_due_dt;

-- NULL CHECK (NEED TO BE SOLVE)
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt IS NULL;
 
-- Check sales, quantity and Price
SELECT
    sls_sales,
    sls_quantity,
    sls_price,
    CASE 
        WHEN sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE
            sls_sales
    END AS new_sls_sales,
    CASE
        WHEN sls_price IS NULL OR sls_price <= 0
            THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
        ELSE 
            sls_price
    END AS new_sls_price
FROM silver.crm_sales_details
WHERE sls_sales <= 0 
    OR sls_sales IS NULL
    OR sls_quantity <= 0 
    OR sls_quantity IS NULL
    OR sls_price <= 0 
    OR sls_price IS NULL
    OR sls_sales != sls_quantity * sls_price
ORDER BY sls_sales, sls_quantity, sls_price;

-- Rules we can follow for transformation
-- sls_price is Negative: Make it positive
-- sls_sales is NULL/Negative/Mispriced : Derive it using sls_sales = sls_quantity * sls_price
-- sls_price is NULL/Zero: Calcualte it from sls_price = sls_sales / sls_quantity

-- ==========================================
-- TABLE: silver.erp_cust_az12
-- ==========================================

-- Check whether the cid is NULL
SELECT 
    cid,
    COUNT(*)
FROM silver.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1;

-- bdate attribute analysis (too old or not born)
SELECT bdate
FROM silver.erp_cust_az12
WHERE bdate > NOW();

-- gen attribute analysis
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

-- ==========================================
-- TABLE: silver.erp_loc_a101
-- ==========================================

-- Check whether the cid is NULL
SELECT 
    cid,q
    COUNT(*)
FROM silver.erp_loc_a101
GROUP BY cid
HAVING COUNT(*) > 1;

SELECT cid
FROM silver.erp_loc_a101
WHERE cid LIKE 'NAS%';

-- cntry attribute
SELECT DISTINCT TRIM(cntry)
FROM silver.erp_loc_a101;

-- ==========================================
-- TABLE: silver.erp_px_cat_g1v2
-- ==========================================

-- Check whether the cid is NULL
SELECT 
    cat,
    COUNT(*)
FROM bronze.erp_px_cat_g1v2
GROUP BY cat
HAVING COUNT(*) > 1;

-- Check unwanted spaces
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Check distinct values of the attributes
SELECT DISTINCT 
    -- cat
    -- subcat
    maintenance
FROM bronze.erp_px_cat_g1v2;