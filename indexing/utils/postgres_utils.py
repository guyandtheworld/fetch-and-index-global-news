import logging
import os
import psycopg2


logging.basicConfig(level=logging.INFO)

params = {
    'database': os.environ["DB_NAME"],
    'user': os.environ["DB_USER"],
    'password': os.environ["DB_PASSWORD"],
    'host': os.environ["DB_HOST"],
    'port': os.environ["DB_PORT"],
}


def insert_stories(stories):
    """
    insert multiple vendors into the vendors table
    """
    sql = """
          INSERT INTO public.apis_story
          (uuid, "entityID_id", "scenarioID_id", title, unique_hash, url,
          search_keyword, published_date, internal_source, "domain",
          "language", source_country, raw_file_source, entry_created)
          VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          """

    conn = None

    logging.info("inserting {} stories".format(len(stories)))
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(sql, stories)
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(error)
        resp = {"status": "error",
                "error": error}
    finally:
        if conn is not None:
            conn.close()
        resp = {"status": "success",
                "data": "inserted {} articles into db".format(len(stories))
                }
    return resp
