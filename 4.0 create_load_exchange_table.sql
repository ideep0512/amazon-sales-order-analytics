-- list @my_internal_stg/exchange/;
LIST @sales_dwh.source.my_internal_stg/exchange/;

use schema common;
create or replace transient table exchange_rate(
    date date, 
    usd2usd decimal(10,7),
    usd2eu decimal(10,7),
    usd2can decimal(10,7),
    usd2uk decimal(10,7),
    usd2inr decimal(10,7),
    usd2jp decimal(10,7)
);

show tables;

COPY INTO sales_dwh.common.exchange_rate
FROM (
    SELECT 
        t.$1::DATE AS exchange_dt,
        to_decimal(t.$2,12,7) AS usd2usd,
        to_decimal(t.$3,12,7) AS usd2eu,
        to_decimal(t.$4,12,7) AS usd2can,
        to_decimal(t.$5,12,7) AS usd2uk,
        to_decimal(t.$6,12,7) AS usd2inr,
        to_decimal(t.$7,12,5) AS usd2jp  
    FROM @source.my_internal_stg/exchange/exchange-rate-data.csv
    (FILE_FORMAT => 'common.my_csv_format') t
);

-- Another way of copying
-- COPY INTO sales_dwh.common.exchange_rate
-- FROM (
--     SELECT 
--         t.$1::DATE AS exchange_rate_dt,
--         t.$2::DECIMAL(10,7) AS usd2usd,
--         t.$3::DECIMAL(10,7) AS usd2eu,
--         t.$4::DECIMAL(10,7) AS usd2can,
--         t.$5::DECIMAL(10,7) AS usd2uk,
--         t.$6::DECIMAL(10,7) AS usd2inr,
--         t.$7::DECIMAL(10,7) AS usd2jp
--     FROM @sales_dwh.source.my_internal_stg/exchange/exchange-rate-data.csv
--     (FILE_FORMAT => 'sales_dwh.common.my_csv_format') t
-- );

-- TRUNCATE TABLE sales_dwh.common.exchange_rate;

SELECT * FROM sales_dwh.common.exchange_rate;
