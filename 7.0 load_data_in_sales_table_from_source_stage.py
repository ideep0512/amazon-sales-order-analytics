import os
import sys
import logging
from dotenv import load_dotenv
load_dotenv()

from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StructField, StringType, LongType, DecimalType, DateType, TimestampType
from snowflake.snowpark.functions import col, lit, row_number, rank
from snowflake.snowpark import Window

# Initiate logging at info level
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s", 
    datefmt="%I:%M:%S"
)

# Snowpark session
def get_snowpark_session() -> Session:
    connection_parameters = {
        "ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
        "USER": os.getenv("SNOWFLAKE_USER"),
        "PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
        "ROLE": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"), 
        "DATABASE": os.getenv("SNOWFLAKE_DATABASE", "SALES_DWH"),
        "SCHEMA": os.getenv("SNOWFLAKE_SCHEMA", "SOURCE"),
        "WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", "SNOWPARK_ETL_WH"),
    }
    return Session.builder.configs(connection_parameters).create()

def test_connection(session):
    try:
        session.sql("SELECT CURRENT_VERSION()").collect()
        logging.info("Snowflake connection successful.")
    except Exception as e:
        logging.error(f"Failed to connect to Snowflake: {e}")
        sys.exit(1)


# Ingest data into IN Sales table
def ingest_in_sales(session) -> None:
    logging.info("Executing COPY INTO for IN Sales Order...")
    
    session.sql("""
        COPY INTO sales_dwh.source.in_sales_order 
        FROM (
            SELECT 
                sales_dwh.source.in_sales_order_seq.nextval,
                t.$1::TEXT AS order_id,
                t.$2::TEXT AS customer_name,
                t.$3::TEXT AS mobile_key,
                t.$4::NUMBER AS order_quantity,
                t.$5::NUMBER AS unit_price,
                t.$6::NUMBER AS order_value,
                t.$7::TEXT AS promotion_code,
                t.$8::NUMBER AS final_order_amount,
                t.$9::NUMBER AS tax_amount,
                t.$10::DATE AS order_dt,
                t.$11::TEXT AS payment_status,
                t.$12::TEXT AS shipping_status,
                t.$13::TEXT AS payment_method,
                t.$14::TEXT AS payment_provider,
                t.$15::TEXT AS mobile,
                t.$16::TEXT AS shipping_address,
                -- metadata$filename AS stg_file_name,
                -- metadata$file_row_number AS stg_row_number,
                -- CAST(metadata$file_row_number AS NUMBER(38,0)) AS stg_row_number,
                -- metadata$file_last_modified AS stg_last_modified,
                metadata$filename::TEXT AS stg_file_name,
                metadata$file_row_number::NUMBER(38,0) AS stg_row_number, 
                metadata$file_last_modified::TIMESTAMP_NTZ AS stg_last_modified
            FROM 
                @sales_dwh.source.my_internal_stg/sales/source=IN/format=csv/
                (file_format => 'sales_dwh.common.my_csv_format') 
            AS t
        ) 
        ON_ERROR = CONTINUE
    """).collect()

    logging.info("IN Sales data ingestion completed successfully.")

# def ingest_in_sales(session) -> None:
#     logging.info("Executing COPY INTO for IN Sales Order...")
    
#     try:
#         session.sql("""
#             COPY INTO sales_dwh.source.in_sales_order (
#                 sales_order_key,
#                 order_id,
#                 customer_name,
#                 mobile_key,
#                 order_quantity,
#                 unit_price,
#                 order_value,
#                 promotion_code,
#                 final_order_amount,
#                 tax_amount,
#                 order_dt,
#                 payment_status,
#                 shipping_status,
#                 payment_method,
#                 payment_provider,
#                 mobile,
#                 shipping_address,
#                 _metadata_file_name,
#                 _metadata_row_number,
#                 _metadata_last_modified
#             )
#             FROM (
#                 SELECT 
#                     sales_dwh.source.in_sales_order_seq.nextval,
#                     t.$1::TEXT AS order_id,
#                     t.$2::TEXT AS customer_name,
#                     t.$3::TEXT AS mobile_key,
#                     t.$4::NUMBER AS order_quantity,
#                     t.$5::NUMBER AS unit_price,
#                     t.$6::NUMBER AS order_value,
#                     t.$7::TEXT AS promotion_code,
#                     t.$8::NUMBER AS final_order_amount,
#                     t.$9::NUMBER AS tax_amount,
#                     t.$10::DATE AS order_dt,
#                     t.$11::TEXT AS payment_status,
#                     t.$12::TEXT AS shipping_status,
#                     t.$13::TEXT AS payment_method,
#                     t.$14::TEXT AS payment_provider,
#                     t.$15::TEXT AS mobile,
#                     t.$16::TEXT AS shipping_address
#                     METADATA$FILENAME::TEXT AS _metadata_file_name,
#                     METADATA$FILE_ROW_NUMBER::NUMBER(38,0) AS _metadata_row_number, 
#                     METADATA$FILE_LAST_MODIFIED::TIMESTAMP_NTZ AS _metadata_last_modified
#                 FROM 
#                     @sales_dwh.source.my_internal_stg/sales/source=IN/format=csv/
#                     (file_format => 'sales_dwh.common.my_csv_format') 
#                 AS t
#             ) 
#             ON_ERROR = CONTINUE
#         """).collect()

#         logging.info("IN Sales data ingestion completed successfully.")
#     except Exception as e:
#         logging.error(f"Error during data ingestion: {e}")
#         sys.exit(1)


def ingest_us_sales(session)-> None:
    session.sql(' \
            copy into sales_dwh.source.us_sales_order                \
            from                                    \
            (                                       \
                select                              \
                sales_dwh.source.us_sales_order_seq.nextval, \
                $1:"Order ID"::text as orde_id,   \
                $1:"Customer Name"::text as customer_name,\
                $1:"Mobile Model"::text as mobile_key,\
                to_number($1:"Quantity") as quantity,\
                to_number($1:"Price per Unit") as unit_price,\
                to_decimal($1:"Total Price") as total_price,\
                $1:"Promotion Code"::text as promotion_code,\
                $1:"Order Amount"::number(10,2) as order_amount,\
                to_decimal($1:"Tax") as tax,\
                $1:"Order Date"::date as order_dt,\
                $1:"Payment Status"::text as payment_status,\
                $1:"Shipping Status"::text as shipping_status,\
                $1:"Payment Method"::text as payment_method,\
                $1:"Payment Provider"::text as payment_provider,\
                $1:"Phone"::text as phone,\
                $1:"Delivery Address"::text as shipping_address,\
                metadata$filename as stg_file_name,\
                metadata$file_row_number as stg_row_numer,\
                metadata$file_last_modified as stg_last_modified\
                from                                \
                    @sales_dwh.source.my_internal_stg/sales/source=US/format=parquet/\
                    (file_format => sales_dwh.common.my_parquet_format)\
                    ) on_error = continue \
            '
            ).collect()
    
def ingest_fr_sales(session)-> None:
    session.sql(' \
        copy into sales_dwh.source.fr_sales_order                                \
        from                                                    \
        (                                                       \
            select                                              \
            sales_dwh.source.fr_sales_order_seq.nextval,         \
            $1:"Order ID"::text as orde_id,                   \
            $1:"Customer Name"::text as customer_name,          \
            $1:"Mobile Model"::text as mobile_key,              \
            to_number($1:"Quantity") as quantity,               \
            to_number($1:"Price per Unit") as unit_price,       \
            to_decimal($1:"Total Price") as total_price,        \
            $1:"Promotion Code"::text as promotion_code,        \
            $1:"Order Amount"::number(10,2) as order_amount,    \
            to_decimal($1:"Tax") as tax,                        \
            $1:"Order Date"::date as order_dt,                  \
            $1:"Payment Status"::text as payment_status,        \
            $1:"Shipping Status"::text as shipping_status,      \
            $1:"Payment Method"::text as payment_method,        \
            $1:"Payment Provider"::text as payment_provider,    \
            $1:"Phone"::text as phone,                          \
            $1:"Delivery Address"::text as shipping_address ,    \
            metadata$filename as stg_file_name,\
            metadata$file_row_number as stg_row_numer,\
            metadata$file_last_modified as stg_last_modified\
            from                                                \
            @sales_dwh.source.my_internal_stg/sales/source=FR/format=json/\
            (file_format => sales_dwh.common.my_json_format)\
            ) on_error=continue\
        '
        ).collect()


# Main function
def main():
    logging.info("Starting Snowflake session...")
    session = get_snowpark_session()
    logging.info("Session created successfully.")

    print("**********Test Connection*************")
    test_connection(session)

    logging.info("Starting data ingestion for IN Sales...")
    print("India Sales Order Before Copy:")
    session.sql("select count(*) from sales_dwh.source.in_sales_order").show()
    ingest_in_sales(session)
    print("India Sales Order After Copy:")
    session.sql("select count(*) from sales_dwh.source.in_sales_order").show()
    logging.info("Data ingestion completed.")


    print("US Sales Order Before Copy:")
    session.sql("select count(*) from sales_dwh.source.us_sales_order").show()
    ingest_us_sales(session)
    print("US Sales Order After Copy:")
    session.sql("select count(*) from sales_dwh.source.us_sales_order").show()

    print("FR Sales Order Before Copy:")
    session.sql("select count(*) from sales_dwh.source.fr_sales_order").show()
    ingest_fr_sales(session)
    print("FR Sales Order After Copy:")
    session.sql("select count(*) from sales_dwh.source.fr_sales_order").show()

if __name__ == '__main__':
    main()
