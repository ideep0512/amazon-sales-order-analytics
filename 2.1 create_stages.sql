-- creating internal stage within source schema.
use schema source;

select current_role();

-- create or replace stage my_internal_stg;
CREATE STAGE IF NOT EXISTS SALES_DWH.SOURCE.my_internal_stg;
SELECT CURRENT_ROLE();

LIST @SALES_DWH.SOURCE.my_internal_stg;

LIST @MY_INTERNAL_STG REFRESH;
SHOW STAGES LIKE 'MY_INTERNAL_STG';

REMOVE @sales_dwh.source.my_internal_stg;
-- completely remove the stage 
DROP STAGE sales_dwh.source.my_internal_stg;
ALTER SESSION SET TIMEZONE = 'Asia/Kolkata';

USE ROLE ACCOUNTADMIN;
ALTER ACCOUNT SET TIMEZONE = 'Asia/Kolkata';
USE ROLE SYSADMIN;

LIST @SALES_DWH.SOURCE.my_internal_stg/exchange;
LIST @SALES_DWH.SOURCE.my_internal_stg/sales;
LIST @SALES_DWH.SOURCE.my_internal_stg/sales/source=FR/;
LIST @SALES_DWH.SOURCE.my_internal_stg/sales/source=IN/;
LIST @SALES_DWH.SOURCE.my_internal_stg/sales/source=US/;








