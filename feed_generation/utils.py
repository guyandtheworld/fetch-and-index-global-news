import logging
import psycopg2
import os

from pandas.io import sql
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)


params = {
    'database': os.environ["DB_NAME"],
    'user': os.environ["DB_USER"],
    'password': os.environ["DB_PASSWORD"],
    'host': os.environ["DB_HOST"],
    'port': os.environ["DB_PORT"],
}


conn_str = f'postgresql://{params["user"]}:{params["password"]}@{params["host"]}:{params["port"]}/{params["database"]}'
connection = create_engine(conn_str)


def connect(query='SELECT version()'):
    """ Connect to the PostgreSQL database server """
    conn = None
    results = []
    try:
        # read connection parameters

        # connect to the PostgreSQL server
        logging.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        logging.info('running : {}'.format(query))
        cur.execute(query)

        # display the PostgreSQL database server version
        results = cur.fetchall()

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(error)
    finally:
        if conn is not None:
            conn.close()

    return results
