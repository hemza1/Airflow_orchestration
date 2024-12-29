import duckdb
import logging

# Configure logging
logging.basicConfig(
    filename='sales_ecom.log',  # Log file
    level=logging.INFO,  # Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def init_db():
    try:
        # Log starting point
        logging.info("Connecting to the DuckDB database: sales_ecom.db")
        
        # Connect to the sales_ecom DuckDB database
        conn = duckdb.connect('sales_ecom.db')
        logging.info("Successfully connected to the database.")

        # Create the raw_layer schema
        create_schema_query = "CREATE SCHEMA IF NOT EXISTS raw_layer;"
        conn.execute(create_schema_query)
        logging.info("Schema 'raw_layer' created or already exists.")

        # Create the refined_layer schema
        create_schema_query = "CREATE SCHEMA IF NOT EXISTS refined_layer;"
        conn.execute(create_schema_query)
        logging.info("Schema 'refined_layer' created or already exists.")

        # Create the reporting_layer schema
        create_schema_query = "CREATE SCHEMA IF NOT EXISTS reporting_layer;"
        conn.execute(create_schema_query)
        logging.info("Schema 'reporting_layer' created or already exists.")

        # Create the sales_ecom table within the raw_layer schema
        create_table_query = """
        CREATE TABLE IF NOT EXISTS raw_layer.sales_ecom (
            OrderID INTEGER,
            Quantity INTEGER,
            OrderDate DATE,
            OrderStatus VARCHAR,
            PaymentMethod VARCHAR,
            CustomerID INTEGER,
            CustomerName VARCHAR,
            Country VARCHAR,
            Email VARCHAR,
            ProductID INTEGER,
            ProductName VARCHAR,
            PricePerUnit DECIMAL(10, 2)
        );
        """
        conn.execute(create_table_query)
        logging.info("Table 'sales_ecom' created successfully in schema 'raw_layer'.")
        conn.execute("""ALTER TABLE raw_layer.sales_ecom 
        ADD COLUMN Ingestion_Time TIMESTAMP;
        """)
        logging.info("Successfully created raw_layer")
        # Verify schema and table creation
        schemas = conn.execute("SHOW SCHEMAS").fetchall()
        tables = conn.execute("SHOW TABLES FROM raw_layer").fetchall()
        logging.info(f"Schemas in database: {schemas}")
        logging.info(f"Tables in 'raw_layer' schema: {tables}")
        

        # Close the database connection
        conn.close()
        logging.info("Database connection closed successfully.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

# Initialize the database
if __name__ == "__main__":
    init_db()
