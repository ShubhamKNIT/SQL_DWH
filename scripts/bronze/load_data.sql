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

TRUNCATE TABLE bronze.crm_cust_info;
COPY bronze.crm_cust_info
FROM '/var/lib/postgresql/datasets/source_crm/cust_info.csv'
DELIMITER ','
CSV HEADER;

TRUNCATE TABLE bronze.crm_prd_info;
COPY bronze.crm_prd_info
FROM '/var/lib/postgresql/datasets/source_crm/prd_info.csv'
DELIMITER ','
CSV HEADER;


TRUNCATE TABLE bronze.crm_sales_details;
COPY bronze.crm_sales_details
FROM '/var/lib/postgresql/datasets/source_crm/sales_details.csv'
DELIMITER ','
CSV HEADER;

TRUNCATE TABLE bronze.erp_cust_az12;
COPY bronze.erp_cust_az12
FROM '/var/lib/postgresql/datasets/source_erp/CUST_AZ12.csv'
DELIMITER ','
CSV HEADER;

TRUNCATE TABLE bronze.erp_loc_a101;
COPY bronze.erp_loc_a101
FROM '/var/lib/postgresql/datasets/source_erp/LOC_A101.csv'
DELIMITER ','
CSV HEADER;

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
COPY bronze.erp_px_cat_g1v2
FROM '/var/lib/postgresql/datasets/source_erp/PX_CAT_G1V2.csv'
DELIMITER ','
CSV HEADER;