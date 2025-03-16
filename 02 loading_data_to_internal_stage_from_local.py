import os
import sys
import logging
from snowflake.snowpark import Session
from dotenv import load_dotenv
load_dotenv()

# initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%I:%M:%S')

# snowpark session
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

    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create() 

def traverse_directory(directory,file_extension) -> list:
    local_file_path = []
    file_name = []  # List to store CSV file paths
    partition_dir = []
    print(directory)
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(file_extension):
                file_path = os.path.join(root, file)
                file_name.append(file)
                partition_dir.append(root.replace(directory, ""))
                local_file_path.append(file_path)
    # print("file_name------->", file_name)
    # print("partition_dir------->",partition_dir)
    # print("local_file_path------->", local_file_path)

    return file_name,partition_dir,local_file_path  


def main():
    try:
        session = get_snowpark_session()
        logging.info("✅ Successfully connected to Snowflake!")

        # Explicitly set the database and schema
        # session.sql("USE DATABASE sales_dwh").collect()
        # session.sql("USE SCHEMA source").collect()

        # Execute a test query
        df = session.sql("SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
        df.show()

        # Specify the directory path to traverse
        # directory_path = '/home/neosoft/my-projects/amazon-sales-order-analytics/data'
        directory_path = 'data'
        csv_file_name, csv_partition_dir , csv_local_file_path= traverse_directory(directory_path,'.csv')
        parquet_file_name, parquet_partition_dir , parquet_local_file_path= traverse_directory(directory_path,'.parquet')
        json_file_name, json_partition_dir , json_local_file_path= traverse_directory(directory_path,'.json')
        stage_location = '@sales_dwh.source.my_internal_stg'

        csv_index = 0
        for file_element in csv_file_name:
            put_result = ( 
                        session.file.put( 
                            csv_local_file_path[csv_index], 
                            stage_location+"/"+csv_partition_dir[csv_index], 
                            auto_compress=False, overwrite=False, parallel=10)
                        )
            print(file_element, " => ", put_result[0].status)
            csv_index+=1

        parquet_index = 0
        for file_element in parquet_file_name:

            put_result = ( 
                        session.file.put( 
                            parquet_local_file_path[parquet_index], 
                            stage_location+"/"+parquet_partition_dir[parquet_index], 
                            auto_compress=False, overwrite=False, parallel=10)
                        )
            print(file_element, " => ", put_result[0].status)
            parquet_index+=1
        
        json_index = 0
        for file_element in json_file_name:

            put_result = ( 
                        session.file.put( 
                            json_local_file_path[json_index], 
                            stage_location+"/"+json_partition_dir[json_index], 
                            auto_compress=False, overwrite=False, parallel=10)
                        )
            print(file_element, " => ", put_result[0].status)
            json_index+=1 
        

    except Exception as e:
        logging.error(f"❌ Connection failed: {str(e)}")

if __name__ == "__main__":
    main()