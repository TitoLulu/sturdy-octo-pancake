import psycopg2
import os 
import logging

from postgres.postgres_sql_queries import drop_table_queries
from postgres.postgres_sql_queries import create_table_queries
from postgres.postgres_db_connection import get_db_connection
from clickhouse.clickhouse_queries import click_drop_table_queries
from clickhouse.clickhouse_queries import click_create_table_queries
from clickhouse.clickhouse_db_connection import get_clickhouse_client

logger = logging.getLogger(__name__)


def drop_tables(cur, conn, client):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    for query in click_drop_table_queries:
        client.command(query)
        
       
def create_tables(cur,conn, client):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    for query in click_create_table_queries:
        client.command(query)
            

def main():
    conn, cur = get_db_connection()
    client = get_clickhouse_client()
    drop_tables(cur=cur, conn=conn, client=client)
    create_tables(cur=cur, conn=conn, client=client)
    conn.close()


if __name__ == '__main__':
    main()

