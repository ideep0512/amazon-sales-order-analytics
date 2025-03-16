SHOW WAREHOUSES;
-- ALTER WAREHOUSE SNOWPARK_ETL_WH RESUME;
ALTER WAREHOUSE SNOWPARK_ETL_WH SET AUTO_RESUME = TRUE;
USE WAREHOUSE SNOWPARK_ETL_WH;
-- create database
create database if not exists sales_dwh;

use database sales_dwh;

create schema if not exists source; -- will have source stage etc
create schema if not exists curated; -- data curation and de-duplication
create schema if not exists consumption; -- fact & dimension
create schema if not exists audit; -- to capture all audit records
create schema if not exists common; -- for file formats sequence object etc
select current_warehouse();
select current_database();
SELECT CURRENT_ROLE();
SELECT CURRENT_USER();
use schema source;

SHOW DATABASES LIKE 'SALES_DWH';
SHOW SCHEMAS IN DATABASE SALES_DWH;


