/*
=== WARNING === 
This script will drop all records in the tables
and insert data afresh.

=== Silver Layer Data Normalization & Standardization ===
Tables:
    - crm_cust_info
    - crm_prd_info
    - crm_sales_details
    - erp_cust_az12
    - erp_loc_a101
    - erp_px_cat_g1v2
*/

-- CALL bronze.load_silver();

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count      int;
    v_table_name text;
BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'SILVER LAYER';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';

    ------------------------------------------------------------------
    -- CRM: Customer Info
    ------------------------------------------------------------------
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'CRM TABLE: crm_cust_info';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';

    v_table_name := 'silver.crm_cust_info';
    RAISE NOTICE 'Processing table: %', v_table_name;
    TRUNCATE silver.crm_cust_info;
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE cst_marital_status
            WHEN 'S' THEN 'Single'
            WHEN 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE cst_gndr
            WHEN 'M' THEN 'Male'
            WHEN 'F' THEN 'Female'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT
            *,
            RANK() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info 
        WHERE cst_id IS NOT NULL
    ) AS t WHERE flag_last = 1;

    SELECT COUNT(*) INTO v_count FROM silver.crm_cust_info;
    RAISE NOTICE 'Loaded crm_cust_info (% rows)', v_count;
    RAISE NOTICE '';

    ------------------------------------------------------------------
    -- CRM: Product Info
    ------------------------------------------------------------------
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'CRM TABLE: crm_prd_info';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';

    v_table_name := 'silver.crm_prd_info';
    RAISE NOTICE 'Processing table: %', v_table_name;
    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE TRIM(prd_line)
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        prd_start_dt,
        LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 Days' AS prd_end_dt
    FROM bronze.crm_prd_info;

    SELECT COUNT(*) INTO v_count FROM silver.crm_prd_info;
    RAISE NOTICE 'Loaded crm_prd_info (% rows)', v_count;
    RAISE NOTICE '';

    ------------------------------------------------------------------
    -- CRM: Sales Details
    ------------------------------------------------------------------
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'CRM TABLE: crm_sales_details';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';

    v_table_name := 'silver.crm_sales_details';
    RAISE NOTICE 'Processing table: %', v_table_name;
    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
            WHEN sls_order_dt <= '0' OR LENGTH(sls_order_dt) != 8 THEN NULL
            ELSE CAST(sls_order_dt AS DATE)
        END AS sls_order_dt,
        CASE
            WHEN sls_ship_dt <= '0' OR LENGTH(sls_ship_dt) != 8 THEN NULL
            ELSE CAST(sls_ship_dt AS DATE)
        END AS sls_ship_dt,
        CASE
            WHEN sls_due_dt <= '0' OR LENGTH(sls_due_dt) != 8 THEN NULL
            ELSE CAST(sls_due_dt AS DATE)
        END AS sls_due_dt,
        CASE
            WHEN sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS new_sls_sales,
        sls_quantity,
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0 THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS new_sls_price
    FROM bronze.crm_sales_details;

    SELECT COUNT(*) INTO v_count FROM silver.crm_sales_details;
    RAISE NOTICE 'Loaded crm_sales_details (% rows)', v_count;
    RAISE NOTICE '';

    ------------------------------------------------------------------
    -- ERP: Customer AZ12
    ------------------------------------------------------------------
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'ERP TABLE: erp_cust_az12';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';

    v_table_name := 'silver.erp_cust_az12';
    RAISE NOTICE 'Processing table: %', v_table_name;
    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid
        END AS cid,
        CASE
            WHEN bdate > NOW() THEN NULL
            ELSE bdate
        END AS bdate,
        CASE
            WHEN TRIM(gen) IN ('M', 'Male') THEN 'Male'
            WHEN TRIM(gen) IN ('F', 'Female') THEN 'Female'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;

    SELECT COUNT(*) INTO v_count FROM silver.erp_cust_az12;
    RAISE NOTICE 'Loaded erp_cust_az12 (% rows)', v_count;
    RAISE NOTICE '';

    ------------------------------------------------------------------
    -- ERP: Location A101
    ------------------------------------------------------------------
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'ERP TABLE: erp_loc_a101';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';

    v_table_name := 'silver.erp_loc_a101';
    RAISE NOTICE 'Processing table: %', v_table_name;
    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
            WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;

    SELECT COUNT(*) INTO v_count FROM silver.erp_loc_a101;
    RAISE NOTICE 'Loaded erp_loc_a101 (% rows)', v_count;
    RAISE NOTICE '';

    ------------------------------------------------------------------
    -- ERP: Product Category G1V2
    ------------------------------------------------------------------
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'ERP TABLE: erp_px_cat_g1v2';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';

    v_table_name := 'silver.erp_px_cat_g1v2';
    RAISE NOTICE 'Processing table: %', v_table_name;
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT 
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

    SELECT COUNT(*) INTO v_count FROM silver.erp_px_cat_g1v2;
    RAISE NOTICE 'Loaded erp_px_cat_g1v2 (% rows)', v_count;
    RAISE NOTICE '';

    -- Completion message
    RAISE NOTICE 'Silver load completed successfully';

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION
            USING
                MESSAGE = 'Silver load failed',
                DETAIL  = format('Table: %, Error: %', v_table_name, SQLERRM),
                HINT    = 'Check data quality, format, or file path';
END;
$$;
