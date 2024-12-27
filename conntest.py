import duckdb

# Connect to your DuckDB database (use ':memory:' for an in-memory database)
conn = duckdb.connect('sales_ecom.duckdb')

# Execute the query to get all table names
tables = conn.execute("SHOW TABLES").fetchall()

# Print the list of table names
for table in tables:
    print(table[0])

# Close the connection
conn.close()
