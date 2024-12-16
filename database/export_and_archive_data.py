import os
import pandas as pd

def ensure_directory_exists(directory_path):
    """Checks if a directory exists, and creates it if it doesn't."""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory created: {directory_path}")
    else:
        print(f"Directory already exists: {directory_path}")

def export_and_archive_data(conn):
    # Ensure the archive directory exists
    archive_dir = './archived_snapshots/'
    ensure_directory_exists(archive_dir)

    # Export data from sales table to a pandas DataFrame
    query = "SELECT * FROM sales"
    sales_df = pd.read_sql(query, conn)

    # Archive the data as a CSV file with timestamp
    timestamp = pd.to_datetime('now').strftime('%Y%m%d_%H%M%S')
    file_path = f'{archive_dir}sales_snapshot_{timestamp}.csv'

    sales_df.to_csv(file_path, index=False)

    print(f"Data exported and archived to: {file_path}")
