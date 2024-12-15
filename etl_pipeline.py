import pandas as pd
import numpy as np
import mysql.connector


def extract():
    orders_csv_path = '../ShopEaseData/orders.csv'
    products_csv_path = '../ShopEaseData/products.csv'

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


    quantities = merged_df['quantity'].to_numpy()
    prices = merged_df['price'].to_numpy()
    revenue = np.multiply(quantities, prices)

    merged_df['revenue'] = revenue

    merged_df['order_date'] = pd.to_datetime(merged_df['order_date'])
    merged_df['year'] = merged_df['order_date'].dt.year
    merged_df['month'] = merged_df['order_date'].dt.month
    merged_df['day'] = merged_df['order_date'].dt.day