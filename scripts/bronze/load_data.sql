/*
=== WARNING === 
This script will drop the all the records
in the table and insert data afresh.

=== CRM Table truncation and data loading ===
Tables:
    - crm_cust_info
    - crm_prd_info
    - crm_sales_details

=== ERP TABLE truncation and data loading ===
Tables:
    - erp_cust_az12
    - erp_loc_a101
    - erp_px_cat_g1v2
*/

-- CALL bronze.load_bronze();

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count      int;
    v_table_name text;
BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'BRONZE LAYER';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';

    ------------------------------------------------------------------
    -- CRM TABLES
    ------------------------------------------------------------------
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'CRM TABLES';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';

    v_table_name := 'bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
    FROM '/var/lib/postgresql/datasets/source_crm/cust_info.csv'
    CSV HEADER;

    SELECT COUNT(*) INTO v_count FROM bronze.crm_cust_info;
    RAISE NOTICE 'Loaded crm_cust_info (% rows)', v_count;
    RAISE NOTICE '';

    v_table_name := 'bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM '/var/lib/postgresql/datasets/source_crm/prd_info.csv'
    CSV HEADER;

    SELECT COUNT(*) INTO v_count FROM bronze.crm_prd_info;
    RAISE NOTICE 'Loaded crm_prd_info (% rows)', v_count;
    RAISE NOTICE '';

    v_table_name := 'bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM '/var/lib/postgresql/datasets/source_crm/sales_details.csv'
    CSV HEADER;

    SELECT COUNT(*) INTO v_count FROM bronze.crm_sales_details;
    RAISE NOTICE 'Loaded crm_sales_details (% rows)', v_count;
    RAISE NOTICE '';

    ------------------------------------------------------------------
    -- ERP TABLES
    ------------------------------------------------------------------
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'ERP TABLES';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';

    v_table_name := 'bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM '/var/lib/postgresql/datasets/source_erp/CUST_AZ12.csv'
    CSV HEADER;

    SELECT COUNT(*) INTO v_count FROM bronze.erp_cust_az12;
    RAISE NOTICE 'Loaded erp_cust_az12 (% rows)', v_count;
    RAISE NOTICE '';

    v_table_name := 'bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM '/var/lib/postgresql/datasets/source_erp/LOC_A101.csv'
    CSV HEADER;

    SELECT COUNT(*) INTO v_count FROM bronze.erp_loc_a101;
    RAISE NOTICE 'Loaded erp_loc_a101 (% rows)', v_count;
    RAISE NOTICE '';

    v_table_name := 'bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM '/var/lib/postgresql/datasets/source_erp/PX_CAT_G1V2.csv'
    CSV HEADER;

    SELECT COUNT(*) INTO v_count FROM bronze.erp_px_cat_g1v2;
    RAISE NOTICE 'Loaded erp_px_cat_g1v2 (% rows)', v_count;
    RAISE NOTICE '';

    RAISE NOTICE 'Bronze load completed successfully';

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION
            USING
                MESSAGE = 'Bronze load failed',
                DETAIL  = format('Table: %, Error: %', v_table_name, SQLERRM),
                HINT    = 'Check CSV file path, permissions, or data format';
END;
$$;
