import pandas as pd
import numpy as np
import mysql.connector


def extract():
    orders_csv_path = '../ShopEaseData/orders.csv'
    products_csv_path = '../ShopEaseData/products.csv'

    orders_df = pd.read_csv(orders_csv_path)
    products_df = pd.read_csv(products_csv_path)

    return orders_df, products_df

