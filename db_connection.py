import psycopg2
import os

DB_NAME = os.getenv("dbname")
USER = os.getenv("user")
PWD = os.getenv("password")

def get_db_connection():
    conn = psycopg2.connect(
            dbname=DB_NAME,
            user=USER,
            password=PWD,
            host="0.0.0.0"
        )
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    return conn, cur

get_db_connection()

