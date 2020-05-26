import logging
import psycopg2
import os
import pandas as pd

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


def add_to_dataframe(articles, query):
    ids_str = "', '".join(articles["uuid"].apply(str).values)
    ids_str = "('{}')".format(ids_str)

    df = pd.read_sql(query.format(ids_str), connection)
    logging.info("fetching stuff: {}".format(df.shape[0]))

    articles = articles.merge(df, how='left',
                              left_on="uuid", right_on="storyID_id")

    articles = articles.dropna()
    logging.info("articles: {}".format(articles.shape[0]))
    articles.drop('storyID_id', axis=1, inplace=True)
    return articles


def generate_story_entities(entities):
    """
    Take in a list of 'processed' stories
    and return the entities found in those stories
    """
    if len(entities) == 0:
        return entities

    entities = entities.astype(str)
    entities = entities.rename({"entityID_id": "entity_id"}, axis=1)

    story_map = {}

    for i in entities['storyID_id'].unique():
        temp = entities[entities['storyID_id'] == i]
        story_map[i] = temp.drop(['storyID_id'], axis=1).to_dict('records')

    return story_map
