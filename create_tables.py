import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop tables if necessary

    Args:
        cur (psycopg2.cursor): Cursor to execute the queries with
        conn (psycopg2.connection): Connection to use
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create staging and final tables

    Args:
        cur (psycopg2.cursor): Cursor to execute the queries with
        conn (psycopg2.connection): Connection to use
    """
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()