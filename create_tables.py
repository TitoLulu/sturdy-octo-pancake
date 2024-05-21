import psycopg2
import os 
import logging

from sql_queries import drop_table_queries
from sql_queries import create_table_queries
from db_connection import get_db_connection

logger = logging.getLogger(__name__)


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
        
       
def create_tables(cur,conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
            

def main():
    conn, cur = get_db_connection()
    drop_tables(cur=cur, conn=conn)
    create_tables(cur=cur, conn=conn)
    conn.close()


if __name__ == '__main__':
    main()

