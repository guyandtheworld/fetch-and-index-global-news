#!/usr/bin/env python
import base64
import logging
import json
import os
import sys

from google.cloud import pubsub_v1
from pathlib import Path


path = Path(os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, "{}".format(path.parent))

from workers.aggregation_worker import start_aggregation  # noqa


logging.basicConfig(level=logging.INFO)


PROJECT_ID = os.getenv("PROJECT_ID", "alrt-ai")
SUBSCRIPTION_NAME = os.getenv("SUBSCRIPTION_NAME", "news_aggregator")


def verify_format(params: dict):
    keys = ["entity_id", "entity_name", "common_names",
            "scenario_id", "source", "date_to", "date_from",
            "storage_bucket", "history_processed"]

    for key in keys:
        if key not in params:
            return None

    params["common_names"] = json.loads(params["common_names"])
    params["source"] = json.loads(params["source"])
    params["history_processed"] = json.loads(params["history_processed"])

    return params


def sub():
    """
    Receives messages from a Pub/Sub subscription.
    """

    client = pubsub_v1.SubscriberClient()
    subscription_path = client.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

    def callback(message):
        logging.info(
            "Received message {} of message ID {}\n".format(
                message, message.message_id
            )
        )

        params = verify_format(json.loads(message.data))
        try:
            start_aggregation(params)
            message.ack()
        except Exception as e:
            logging.info(
                "message processing failed. up for retry. - " + str(e))

    streaming_pull_future = client.subscribe(
        subscription_path, callback=callback
    )
    logging.info("Listening for messages on {}..\n".format(subscription_path))

    try:
        streaming_pull_future.result()
    except:  # noqa
        streaming_pull_future.cancel()


if __name__ == "__main__":
    sub()
