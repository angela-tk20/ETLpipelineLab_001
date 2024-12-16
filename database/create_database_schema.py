def create_database_schema(conn, cursor):
    cursor.execute("CREATE DATABASE IF NOT EXISTS etl_pipeline")
    cursor.execute("USE etl_pipeline")

    # Drop the 'sales' table if it exists to start fresh
    cursor.execute("DROP TABLE IF EXISTS sales")

    # Create the 'sales' table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            order_id INT PRIMARY KEY,
            customer_id INT,
            order_date DATE,
            product_id INT,
            quantity INT,
            revenue FLOAT,
            year INT,
            month INT,
            day INT
        )
    """)

    conn.commit()
    print("Database schema created successfully (sales table dropped and recreated).")
