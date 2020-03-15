import json
import logging
import os
import shutil

from google.cloud import pubsub_v1
from google.cloud import storage

from utils.helpers import process_company_json
from data.postgres_utils import insert_stories


logging.basicConfig(level=logging.INFO)


DESTINATION_FOLDER = "temp"
PROJECT_ID = os.getenv("PROJECT_ID", "alrt-ai")
SUBSCRIPTION_NAME = os.getenv("SUBSCRIPTION_NAME", "index_articles")
BUCKET_NAME = "news_staging_bucket"


def index_file(bucket, params: dict):
    logging.info("processing record: {}".format(params["source_file"]))

    results = process_company_json(params, bucket)

    if isinstance(results, list) and len(results) > 0:
        logging.info("writing {} articles, from entity {} into db".format(
            len(results), params['entity_id']))
        resp = insert_stories(results)
    elif isinstance(results, list):
        resp = {"status": "success",
                "data": "no articles to insert"}
    else:
        resp = {"status": "error",
                "data": "something wrong"}

    return resp


def verify_format(params: dict):
    keys = ["entity_id", "scenario_id", "history_processed",
            "last_tracked", "source_file",
            "storage_bucket", "source"]

    for key in keys:
        if key not in params:
            return None

    params["history_processed"] = json.loads(params["history_processed"])
    return params


def subscriber():
    """
    indexes articles by subscribing to news aggregator output
    """
    # setup connection to database

    logging.info("loading storage client")
    storage_client = storage.Client()

    if os.path.exists(DESTINATION_FOLDER):
        shutil.rmtree(DESTINATION_FOLDER)

    bucket = storage_client.bucket(BUCKET_NAME)
    os.mkdir(DESTINATION_FOLDER)

    client = pubsub_v1.SubscriberClient()

    subscription_path = client.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

    def callback(message):
        logging.info(
            "Received message {} of message ID {}\n".format(
                message, message.message_id
            )
        )

        params = {}
        if message.attributes:
            for key in message.attributes:
                value = message.attributes.get(key)
                params[key] = value

        params["source_file"] = message.data

        params = verify_format(params)

        if params:
            try:
                response = index_file(bucket, params)
                logging.info(response)
                if response["status"] == "success":
                    message.ack()
                    logging.info(
                        "{} written & ack-ed".format(params["source_file"]))
                else:
                    logging.info("can't ack message")
            except Exception as e:
                logging.info(
                    "message processing failed. up for retry. - " + str(e))
        else:
            logging.info("message format broken")

    streaming_pull_future = client.subscribe(
        subscription_path, callback=callback
    )
    logging.info("Listening for messages on {}..\n".format(subscription_path))

    try:
        streaming_pull_future.result()
    except:  # noqa
        streaming_pull_future.cancel()


if __name__ == "__main__":
    subscriber()
