import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load data into staging tables

    Args:
        cur (psycopg2.cursor): Cursor to execute the queries with
        conn (psycopg2.connection): Connection to use
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging tables into final tables.

    Args:
        cur (psycopg2.cursor): Cursor to execute the queries with
        conn (psycopg2.connection): Connection to use
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={host} dbname={db_name} user={db_user} password={db_password} port={db_port}".format(
            **config["CLUSTER"]
        )
    )
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()