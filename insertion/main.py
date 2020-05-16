import base64
import logging
import os
import psycopg2
import json


logging.basicConfig(level=logging.INFO)

params = {
    'database': os.environ["DB_NAME"],
    'user': os.environ["DB_USER"],
    'password': os.environ["DB_PASSWORD"],
    'host': os.environ["DB_HOST"],
    'port': os.environ["DB_PORT"],
}


def insert_into_database(query, values):
    """
    insert values we get on the payload into the database
    """

    conn = None

    logging.info("inserting {} values".format(len(values)))
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(query, values)
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(error)
        resp = {"success": False,
                "error": error}
    finally:
        if conn is not None:
            conn.close()
        resp = {"success": True,
                "data": "inserted {} articles into db".format(len(values))
                }
    return resp


def insertion(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """

    logging.info("""This Function was triggered by messageId {} published at {}

    """.format(context.event_id, context.timestamp))

    logging.info("loading storage client")

    # data could be wrong format
    if 'data' in event:
        str_params = base64.b64decode(event['data']).decode('utf-8')
        data = json.loads(str_params)
    else:
        logging.info("no text in the message")
        return

    logging.info("Inserting {} values from the {} service".format(
        len(data["data"]), data["source"]))

    if data:
        response = insert_into_database(data["query"], data["data"])
        logging.info(response)
        if response["success"]:
            logging.info(response["data"])
        else:
            logging.info("can't ack message")
    else:
        logging.info("message format broken")
