import psycopg2
import os

DB_NAME = os.getenv("POSTGRES_DB")
USER = os.getenv("POSTGRES_USER")
PWD = os.getenv("POSTGRES_PASSWORD")


def get_db_connection():
    conn = psycopg2.connect(
            dbname=DB_NAME,
            user=USER,
            password=PWD,
            host='0.0.0.0',
            port='5433'
        )
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    return conn, cur

def main():
    get_db_connection()

if __name__ == '__main__':
    get_db_connection()

