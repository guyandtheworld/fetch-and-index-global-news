import base64
import json
import logging
import os

from google.cloud import storage

from utils.helpers import process_company_json
from utils.publisher import publish_stories


logging.basicConfig(level=logging.INFO)


PROJECT_ID = os.getenv("PROJECT_ID", "alrt-ai")
SUBSCRIPTION_NAME = os.getenv("SUBSCRIPTION_NAME", "index_articles")
BUCKET_NAME = "news_staging_bucket"


def index_file(bucket, params: dict):
    logging.info("processing record: {}".format(params["source_file"]))

    results = process_company_json(params, bucket)

    if isinstance(results, list) and len(results) > 0:
        logging.info("writing {} articles, from entity {} into db".format(
            len(results), params['entity_id']))
        resp = publish_stories(results)
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


def test_index():
    params = {"source_file": "3f210a21-fa94-499e-8b60-ada4f9ef5cf6-COVID/google_news/2020-05-15T05:22:10Z-2020-05-15T11:22:07Z.json",
              "entity_id": "3f210a21-fa94-499e-8b60-ada4f9ef5cf6",
              "scenario_id": "d3ef747b-1c3e-4582-aecb-eacee1cababe",
              "history_processed": "true",
              "last_tracked": "2020-04-22T12:16:41Z",
              "storage_bucket": "news_staging_bucket",
              "source": "google_news"}
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    index_file(bucket, params)


def index(event, context):
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
    storage_client = storage.Client()

    bucket = storage_client.bucket(BUCKET_NAME)

    if 'data' in event:
        str_params = base64.b64decode(event['data']).decode('utf-8')
        params = json.loads(str_params)
    else:
        logging.info("no text in the message")
        return

    if params:
        try:
            response = index_file(bucket, params)
            logging.info(response)
            if response:
                logging.info(
                    "{} written & ack-ed".format(params["source_file"]))
            else:
                logging.info("can't ack message")
        except Exception as e:
            logging.info(
                "message processing failed. up for retry. - " + str(e))
    else:
        logging.info("message format broken")
