import mysql.connector
from dotenv import load_dotenv
import os

def connectdb():
    load_dotenv()
    conn = mysql.connector.connect(
        host=os.getenv("HOST"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
    )
    cursor = conn.cursor()
    return conn, cursor