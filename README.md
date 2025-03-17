Amazon Sales Order Analytics - End-to-End Data Engineering Project

This project is a complete end-to-end data engineering pipeline for analyzing Amazon sales orders using Snowflake Snowpark.

Project Overview
The pipeline involves:
1. Extracting sales data in CSV, JSON, and Parquet formats.
2. Loading data into Snowflake internal staging.
3. Transforming data using Snowpark DataFrames.
4. Building a data warehouse with a dimensional model.
5. Performing analytics on Amazon sales.

Tech Stack
Snowflake Snowpark (Data Processing)
Python (Data Ingestion & Transformation)
Pandas & PySpark (Data Analysis)

Future Improvements
Automate data ingestion with Airflow
Implement dbt for data transformations
Integrate BI tools like Tableau or Power BI


using python 3.10.16
downgraded to snowflake-snowpark-python==1.9.0
pip install snowflake-connector-python[pandas]

pyenv install 3.12.2  # Install Python 3.12
pyenv local 3.12.2    # Set it as the local version
python -m venv my_env  # Create a virtual environment
source my_env/bin/activate  # Activate it
pip install snowflake-snowpark-python  # Install Snowpark

