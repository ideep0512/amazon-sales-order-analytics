import os
import sys
import logging
from dotenv import load_dotenv
load_dotenv()

from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import col,lit,row_number, rank
from snowflake.snowpark import Window

# initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%I:%M:%S')

# snowpark session
def get_snowpark_session() -> Session:
    logging.info("Initializing Snowflake session...")
    connection_parameters = {
        "ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
        "USER": os.getenv("SNOWFLAKE_USER"),
        "PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
        "ROLE": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"), 
        "DATABASE": os.getenv("SNOWFLAKE_DATABASE", "SALES_DWH"),
        "SCHEMA": "SOURCE",
        "WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", "SNOWPARK_ETL_WH"),
    }
    logging.info("Snowflake session established successfully.")
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()

def filter_dataset(df, column_name, filter_criterian) -> DataFrame:
    # Payment Status = Paid
    # Shipping = Delivered
    logging.info(f"Filtering dataset: {column_name} = {filter_criterian}")
    return_df = df.filter(col(column_name) == filter_criterian)

    return return_df

def main():
    logging.info("Starting data processing...")
    #get the session object and get dataframe
    session = get_snowpark_session()
    sales_df = session.sql("select * from sales_dwh.source.in_sales_order")
    # print(sales_df.show(3))

    # apply filter to select only paid and delivered sale orders
    # select * from in_sales_order where PAYMENT_STATUS = 'Paid' and SHIPPING_STATUS = 'Delivered'
    paid_sales_df = filter_dataset(sales_df,'PAYMENT_STATUS','Paid')
    shipped_sales_df = filter_dataset(paid_sales_df,'SHIPPING_STATUS','Delivered')

    # adding country and region to the data frame
    # select *, 'IN' as Country, 'APAC' as Region from in_sales_order where PAYMENT_STATUS = 'Paid' and SHIPPING_STATUS = 'Delivered'
    country_sales_df = shipped_sales_df.with_column('Country',lit('IN')).with_column('Region',lit('APAC'))

    # join to add forex calculation
    forex_df = session.sql("select * from sales_dwh.common.exchange_rate")
    # forex_df.show(2)
    sales_with_forext_df = country_sales_df.join(forex_df,country_sales_df['order_dt']==forex_df['date'],join_type='outer')
    # sales_with_forext_df.show(2)

    # #de-duplication
    # print(sales_with_forext_df.count())
    unique_orders = sales_with_forext_df.with_column('order_rank',rank().over(Window.partitionBy(col("order_dt")).order_by(col('_metadata_last_modified').desc()))).filter(col("order_rank")==1).select(col('SALES_ORDER_KEY').alias('unique_sales_order_key'))
    final_sales_df = unique_orders.join(sales_with_forext_df,unique_orders['unique_sales_order_key']==sales_with_forext_df['SALES_ORDER_KEY'],join_type='inner')
    final_sales_df = final_sales_df.select(
        col('SALES_ORDER_KEY'),
        col('ORDER_ID'),
        col('ORDER_DT'),
        col('CUSTOMER_NAME'),
        col('MOBILE_KEY'),
        col('Country'),
        col('Region'),
        col('ORDER_QUANTITY'),
        lit('INR').alias('LOCAL_CURRENCY'),
        col('UNIT_PRICE').alias('LOCAL_UNIT_PRICE'),
        col('PROMOTION_CODE').alias('PROMOTION_CODE'),
        col('FINAL_ORDER_AMOUNT').alias('LOCAL_TOTAL_ORDER_AMT'),
        col('TAX_AMOUNT').alias('local_tax_amt'),
        col('USD2INR').alias("Exhchange_Rate"),
        (col('FINAL_ORDER_AMOUNT')/col('USD2INR')).alias('US_TOTAL_ORDER_AMT'),
        (col('TAX_AMOUNT')/col('USD2INR')).alias('USD_TAX_AMT'),
        col('payment_status'),
        col('shipping_status'),
        col('payment_method'),
        col('payment_provider'),
        col('mobile').alias('conctact_no'),
        col('shipping_address')
    )
    logging.info("Data transformation completed.")
    logging.info(f"Final record count before saving: {final_sales_df.count()}")
    logging.info("Saving final transformed data to sales_dwh.curated.in_sales_order...")
    #final_sales_df.show(5)
    final_sales_df.write.save_as_table("sales_dwh.curated.in_sales_order",mode="append")
    logging.info("Data successfully saved to curated schema.")


if __name__ == '__main__':
    main()