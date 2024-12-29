from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
import logging
import duckdb

# Configuration
DUCKDB_FILE = "db/sales_ecom.db"
RAW_TABLE = "raw_layer.sales_ecom"
REFINED_TABLE = "refined_layer.sales_ecom"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Step 1: Create Refined Table
def create_refined_table(**kwargs):
    """
    Create the refined table in DuckDB.
    """
    try:
        conn = duckdb.connect(DUCKDB_FILE)
        logging.info("Creating refined layer table if it doesn't exist.")

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {REFINED_TABLE} (
            OrderID INT NOT NULL,
            Quantity INT NOT NULL,
            OrderDate DATE NOT NULL,
            TotalPrice DOUBLE,
            OrderStatus VARCHAR,
            PaymentMethod VARCHAR,
            CustomerID INT NOT NULL,
            CustomerName VARCHAR,
            Country VARCHAR,
            ProductID INT NOT NULL,
            ProductName VARCHAR,
            PricePerUnit DOUBLE,
            IngestionTime TIMESTAMP
        );
        """
        conn.execute(create_table_query)
        logging.info(f"Refined table '{REFINED_TABLE}' created successfully.")
        conn.close()
    except Exception as e:
        logging.error(f"Error while creating refined table: {e}")
        raise

# Step 2: Process Data from Raw to Refined
def process_and_load_data(**kwargs):
    """
    Process data from the raw layer and insert it into the refined layer.
    """
    try:
        conn = duckdb.connect(DUCKDB_FILE)
        logging.info("Fetching new data from raw layer.")

        # Get the latest ingestion_time from the refined layer
        max_ingestion_query = f"""
        SELECT COALESCE(MAX(IngestionTime), '1970-01-01 00:00:00') AS last_ingestion
        FROM {REFINED_TABLE};
        """
        last_ingestion_time = conn.execute(max_ingestion_query).fetchone()[0]
        logging.info(f"Last processed ingestion_time: {last_ingestion_time}")

        # Fetch new data from the raw layer
        fetch_query = f"""
        SELECT * ,
               Quantity * PricePerUnit AS TotalPrice
        FROM {RAW_TABLE}
        WHERE ingestion_time > '{last_ingestion_time}';
        """
        new_data = conn.execute(fetch_query).fetchdf()

        if new_data.empty:
            logging.info("No new data to process.")
        else:
            logging.info(f"Processing {len(new_data)} new records.")

            # Insert the processed data into the refined table
            insert_query = f"""
            INSERT INTO {REFINED_TABLE} (
                OrderID, Quantity, OrderDate, TotalPrice, OrderStatus,
                PaymentMethod, CustomerID, CustomerName, Country,
                ProductID, ProductName, PricePerUnit, IngestionTime
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            logging.info(f"Columns in new data: {new_data.columns}")
            logging.info(f"First few rows in new data:\n{new_data.head()}")

            conn.executemany(insert_query, new_data.values.tolist())
            logging.info(f"Inserted {len(new_data)} records into the refined table.")

        conn.close()
    except Exception as e:
        logging.error(f"Error during refined layer processing: {e}")
        raise

# Define the DAG
with DAG(
    dag_id="refined_layer_sales_data",
    default_args={"retries": 2},
    description="Refine sales data from raw layer to refined layer in DuckDB",
    start_date=datetime(2022, 12, 1),
    schedule_interval='* * * * *',  # Every minute
    catchup=False
) as dag:

    # Task 1: Wait for the `sales_raw_layer` DAG to complete
    wait_for_raw_layer = ExternalTaskSensor(
        task_id="wait_for_raw_layer",
        external_dag_id="fetch_and_load_sales_data",  
        external_task_id=None,  
        mode="poke",  
        timeout=3600, 
        retries=2,
    )

    # Task 2: Create Refined Table
    create_refined_table_task = PythonOperator(
        task_id="create_refined_table",
        python_callable=create_refined_table
    )

    # Task 3: Process and Load Data
    process_and_load_data_task = PythonOperator(
        task_id="process_and_load_data",
        python_callable=process_and_load_data
    )

    # Task dependencies
    wait_for_raw_layer >> create_refined_table_task >> process_and_load_data_task
