import os
import clickhouse_connect

DB_NAME = os.getenv("CLICKHOUSE_DB")
USER = os.getenv("CLICKHOUSE_USER")
PWD = os.getenv("CLICKHOUSE_PASSWORD")

def get_clickhouse_client():

    client = clickhouse_connect.get_client(
        host='localhost',
        port='8123',
        database=DB_NAME,
        username=USER,
        password=PWD
    )

    return client

def main():
    client = get_clickhouse_client()
   

if __name__ == '__main__':
    main()
