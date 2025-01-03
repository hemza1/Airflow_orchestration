from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import requests
import duckdb
import pandas as pd
from io import StringIO

# API and file details
API_URL = "https://my.api.mockaroo.com/sales_ecom"
API_KEY = "d805bc00"
DUCKDB_FILE = "db/sales_ecom.db"
DUCKDB_TABLE = "raw_layer.sales_ecom"
CSV_FILE_PATH = "/tmp/sales_ecom.csv"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Fetch CSV data from the API and save it locally
def fetch_csv_data_from_api(**kwargs):
    """
    Fetch CSV data from the API and return it as a list of dictionaries.
    This data is **pushed** to XCom automatically when returned.
    """
    try:
        headers = {"X-API-Key": API_KEY}
        logging.info(f"Fetching data from API: {API_URL}")
        response = requests.get(API_URL, headers=headers)

        if response.status_code == 200:
            # Decode and load CSV data directly into a pandas DataFrame using StringIO
            csv_content = response.content.decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))
            logging.info("CSV data successfully loaded into a pandas DataFrame.")

            # Convert the DataFrame to a list of dictionaries
            data_dicts = df.to_dict(orient='records')
            logging.debug(f"Fetched data: {data_dicts}")

            # Return data, which will be pushed to XCom
            return data_dicts
        else:
            logging.error(f"Failed to fetch data. Status code: {response.status_code}, Response: {response.text}")
            raise Exception(f"API request failed with status code {response.status_code}.")
    except Exception as e:
        logging.error(f"Error while fetching data from API: {e}")
        raise

# Insert the fetched CSV data into DuckDB
def insert_records_to_duckdb(**kwargs):
    """
    Insert a list of dictionaries into the DuckDB table using parameterized queries.
    This function **pulls** data from XCom using task_instance.xcom_pull()
    """
    try:
        # Pull the data from XCom
        task_instance = kwargs['ti']
        data_dicts = task_instance.xcom_pull(task_ids='fetch_csv_data')

        # Check if data exists
        if not data_dicts:
            raise ValueError("No data fetched, aborting insertion.")

        # Connect to DuckDB
        conn = duckdb.connect(DUCKDB_FILE)
        logging.info(f"Connected to DuckDB file: {DUCKDB_FILE}")

        # Prepare an INSERT query
        insert_query = f"""
        INSERT INTO raw_layer.sales_ecom (
            OrderID, Quantity, OrderDate, OrderStatus, PaymentMethod,
            CustomerID, CustomerName, Country, Email, ProductID, ProductName, PricePerUnit , Ingestion_Time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Insert records using parameterized queries
        for record in data_dicts:
            Ingestion_Time = datetime.now()
            conn.execute(
                insert_query, 
                [
                    record['OrderID'], record['Quantity'], record['OrderDate'], record['OrderStatus'],
                    record['PaymentMethod'], record['CustomerID'], record['CustomerName'], record['Country'],
                    record['Email'], record['ProductID'], record['ProductName'], record['PricePerUnit'],Ingestion_Time
                ]
            )
        logging.info(f"Successfully inserted {len(data_dicts)} records into {DUCKDB_TABLE}.")

        # Close the connection
        conn.close()
    except Exception as e:
        logging.error(f"Error while inserting records into DuckDB: {e}")
        raise

# Define the Airflow DAG
with DAG(
    dag_id="fetch_and_load_sales_data",
    default_args={"retries": 2},
    description="Fetch sales data from API and store into DuckDB",
    start_date=datetime(2022, 12, 1),
    schedule_interval='* * * * *',  # Every minute
    catchup=False
) as dag:

    # Task 1: Fetch data from API
    fetch_data_task = PythonOperator(
        task_id="fetch_csv_data",
        python_callable=fetch_csv_data_from_api
    )

    # Task 2: Store data into DuckDB
    store_data_task = PythonOperator(
        task_id="insert_records_to_duckdb",
        python_callable=insert_records_to_duckdb
    )

    # Task dependencies
    fetch_data_task >> store_data_task
