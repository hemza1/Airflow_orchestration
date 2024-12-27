import logging
import requests
import duckdb
import pandas as pd
from io import StringIO

API_URL = "https://my.api.mockaroo.com/sales_ecom"
API_KEY = "d805bc00"
DUCKDB_FILE = "C:/Users/hp/Downloads/data_eng/init/db/sales_ecom.db"
DUCKDB_TABLE = "raw_layer.sales_ecom"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def fetch_csv_data_from_api():
    """
    Fetch CSV data from the API and return it as a list of dictionaries.
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
            return data_dicts
        else:
            logging.error(f"Failed to fetch data. Status code: {response.status_code}, Response: {response.text}")
            raise Exception(f"API request failed with status code {response.status_code}.")
    except Exception as e:
        logging.error(f"Error while fetching data from API: {e}")
        raise


def insert_records_to_duckdb(data_dicts):
    """
    Insert a list of dictionaries into the DuckDB table using parameterized queries.
    """
    try:
        # Connect to DuckDB
        conn = duckdb.connect(DUCKDB_FILE)
        logging.info(f"Connected to DuckDB file: {DUCKDB_FILE}")

        # Prepare an INSERT query
        insert_query = f"""
        INSERT INTO raw_layer.sales_ecom (
            OrderID, Quantity, OrderDate, OrderStatus, PaymentMethod,
            CustomerID, CustomerName, Country, Email, ProductID, ProductName, PricePerUnit
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Insert records using parameterized queries
        for record in data_dicts:
            conn.execute(
                insert_query, 
                [
                    record['OrderID'], record['Quantity'], record['OrderDate'], record['OrderStatus'],
                    record['PaymentMethod'], record['CustomerID'], record['CustomerName'], record['Country'],
                    record['Email'], record['ProductID'], record['ProductName'], record['PricePerUnit']
                ]
            )
        logging.info(f"Successfully inserted {len(data_dicts)} records into {DUCKDB_TABLE}.")

        # Close the connection
        conn.close()
    except Exception as e:
        logging.error(f"Error while inserting records into DuckDB: {e}")
        raise

# Fetch and print the DataFrame
data = fetch_csv_data_from_api()
print(data)

# Insert data into DuckDB
insert_records_to_duckdb(data)