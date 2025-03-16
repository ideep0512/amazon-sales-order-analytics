select current_schemas();
show schemas;
USE SCHEMA SALES_DWH.SOURCE;
show sequences;
-- -- India Sales Table in Source Schema (CSV File)
-- CREATE OR REPLACE TRANSIENT TABLE SALES_DWH.SOURCE.in_sales_order (
--     sales_order_key NUMBER(38,0),
--     order_id VARCHAR(50),  
--     customer_name VARCHAR(100),  
--     mobile_key VARCHAR(20),  
--     order_quantity NUMBER(38,0),
--     unit_price NUMBER(38,2),  
--     order_value NUMBER(38,2),  
--     promotion_code VARCHAR(20),
--     final_order_amount NUMBER(10,2),
--     tax_amount NUMBER(10,2),
--     order_dt DATE,
--     payment_status VARCHAR(20),
--     shipping_status VARCHAR(20),
--     payment_method VARCHAR(30),
--     payment_provider VARCHAR(30),
--     mobile VARCHAR(20),
--     shipping_address VARCHAR(255)  
--     -- _metadata_file_name VARCHAR(255),
--     -- _metadata_row_number NUMBER(38,0), 
--     -- _metadata_last_modified TIMESTAMP_NTZ(9)
-- );

CREATE OR REPLACE TRANSIENT TABLE SALES_DWH.SOURCE.in_sales_order (
    sales_order_key NUMBER, 
    order_id VARCHAR,  
    customer_name VARCHAR,  
    mobile_key VARCHAR,  
    order_quantity NUMBER,
    unit_price NUMBER,  
    order_value NUMBER,  
    promotion_code VARCHAR,
    final_order_amount NUMBER,
    tax_amount NUMBER,
    order_dt DATE,
    payment_status VARCHAR,
    shipping_status VARCHAR,
    payment_method VARCHAR,
    payment_provider VARCHAR,
    mobile VARCHAR,
    shipping_address VARCHAR,  
    _metadata_file_name VARCHAR,
    _metadata_row_number NUMBER, 
    _metadata_last_modified TIMESTAMP_NTZ
);

SELECT * FROM SALES_DWH.SOURCE.in_sales_order;

USE SCHEMA SALES_DWH.SOURCE;
-- US Sales Table in Source Schema (Parquet File)
CREATE OR REPLACE TRANSIENT TABLE SALES_DWH.SOURCE.us_sales_order (
    sales_order_key NUMBER, 
    order_id VARCHAR,  
    customer_name VARCHAR,  
    mobile_key VARCHAR,  
    order_quantity NUMBER,
    unit_price NUMBER,  
    order_value NUMBER,  
    promotion_code VARCHAR,
    final_order_amount NUMBER,
    tax_amount NUMBER,
    order_dt DATE,
    payment_status VARCHAR,
    shipping_status VARCHAR,
    payment_method VARCHAR,
    payment_provider VARCHAR,
    phone VARCHAR,  
    shipping_address VARCHAR,
    _metadata_file_name VARCHAR,
    _metadata_row_number NUMBER,  
    _metadata_last_modified TIMESTAMP_NTZ
);

SELECT * FROM SALES_DWH.SOURCE.us_sales_order;

USE SCHEMA SALES_DWH.SOURCE;

-- France Sales Table in Source Schema (JSON File)
CREATE OR REPLACE TRANSIENT TABLE SALES_DWH.SOURCE.fr_sales_order (
    sales_order_key NUMBER, 
    order_id VARCHAR,  
    customer_name VARCHAR,  
    mobile_key VARCHAR,  
    order_quantity NUMBER,
    unit_price NUMBER,  
    order_value NUMBER,  
    promotion_code VARCHAR,
    final_order_amount NUMBER,
    tax_amount NUMBER,
    order_dt DATE,
    payment_status VARCHAR,
    shipping_status VARCHAR,
    payment_method VARCHAR,
    payment_provider VARCHAR,
    phone VARCHAR,  
    shipping_address VARCHAR,
    _metadata_file_name VARCHAR,
    _metadata_row_number NUMBER,  
    _metadata_last_modified TIMESTAMP_NTZ
);
SELECT * FROM SALES_DWH.SOURCE.fr_sales_order;

SHOW TABLES;

SELECT count(*) FROM sales_dwh.source.in_sales_order;
select * from sales_dwh.source.in_sales_order;
TRUNCATE TABLE sales_dwh.source.in_sales_order;

LIST @sales_dwh.source.my_internal_stg/sales/source=IN/format=csv/;
SELECT COUNT(*)
FROM @sales_dwh.source.my_internal_stg/sales/source=IN/format=csv/
(FILE_FORMAT => 'sales_dwh.common.my_csv_format');

DESC TABLE sales_dwh.source.in_sales_order;

SELECT *
FROM TABLE(VALIDATE_PIPE_LOAD(
    'SALES_DWH.SOURCE.MY_INTERNAL_STG'
))
ORDER BY LAST_ERROR_TIME DESC
LIMIT 10;

SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'IN_SALES_ORDER', -- Target table name
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP()) -- Look back 24 hours
));







