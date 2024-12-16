import pandas as pd
import numpy as np

from database.connectdb import connectdb
from database.create_database_schema import create_database_schema
from database.load_data_into_db import load_data_into_db


def extract():
    orders_csv_path = './csv_files/orders.csv'
    products_csv_path = './csv_files/products.csv'

    orders_df = pd.read_csv(orders_csv_path)
    products_df = pd.read_csv(products_csv_path)

    return orders_df, products_df


def transform(orders_df, products_df):
    merged_df = pd.merge(orders_df, products_df, on='product_id', how='left')

    # Handle missing values
    merged_df = merged_df.dropna()

    # Convert data types
    merged_df['order_date'] = pd.to_datetime(merged_df['order_date'])
    merged_df['quantity'] = merged_df['quantity'].astype(int)
    merged_df['price'] = merged_df['price'].astype(float)

    # Calculate revenue
    quantities = merged_df['quantity'].to_numpy()
    prices = merged_df['price'].to_numpy()
    revenue = np.multiply(quantities, prices)
    merged_df['revenue'] = revenue

    # Extract year, month, and day from the order_date
    merged_df['order_date'] = pd.to_datetime(merged_df['order_date'])
    merged_df['year'] = merged_df['order_date'].dt.year
    merged_df['month'] = merged_df['order_date'].dt.month
    merged_df['day'] = merged_df['order_date'].dt.day

    return merged_df


def load(dataframe):
    conn, cursor = connectdb()

    try:
        create_database_schema(conn, cursor)
        load_data_into_db(conn, cursor, dataframe)

    except Exception as e:
        print(f"error creating database schema or loading into the database : {e}")

    finally:
        cursor.close()
        conn.close()


def run_etl_pipeline():
    # Extract data
    orders_df, products_df = extract()

    #  Transform data
    dataframe = transform(orders_df, products_df)

    # Load data into the database
    load(dataframe)


if __name__ == '__main__':
    run_etl_pipeline()
