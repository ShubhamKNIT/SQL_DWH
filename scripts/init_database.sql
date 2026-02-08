/*
=== Purpose ===

This SQL script initializes the Data Warehouse database and 
creates the necessary schemas for the bronze, silver, and gold layers.

=== Warning ===
Running this script will drop the existing DataWarehouse database if it exists,
and all data within it will be lost. Use with caution.
*/


-- docker exec -it [container] psql -U postgres -d postgres

-- Initialize the Data Warehouse database
DROP DATABASE IF EXISTS DataWarehouse;
CREATE DATABASE DataWarehouse;
\c DataWarehouse;

-- Schemas for the data warehouse
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;