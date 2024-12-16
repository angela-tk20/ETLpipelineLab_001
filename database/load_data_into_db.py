import pandas as pd

from database.export_and_archive_data import export_and_archive_data


def load_data_into_db(conn, cursor, df):
    insert_query = """
        INSERT INTO sales (order_id, customer_id, order_date, product_id, quantity, revenue, year, month, day)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Track any failed insertions
    failed_rows = []

    for _, row in df.iterrows():
        try:
            cursor.execute(insert_query, (
                row['order_id'], row['customer_id'], row['order_date'],
                row['product_id'], row['quantity'], row['revenue'],
                row['year'], row['month'], row['day']
            ))
        except Exception as e:
            print(f"Failed to insert row {row['order_id']}: {e}")
            failed_rows.append(row)

    conn.commit()


    print(f"Inserted {len(df) - len(failed_rows)} rows successfully.")
    print(f"Failed to insert {len(failed_rows)} rows.")

    # After loading data, export and archive
    export_and_archive_data(conn)

