import logging
import os

from google.cloud import pubsub_v1
from utils.helpers import extract_body


logging.basicConfig(level=logging.INFO)


PROJECT_ID = os.getenv("PROJECT_ID", "alrt-ai")
SUBSCRIPTION_NAME = os.getenv("SUBSCRIPTION_NAME", "body_extractor")


def subscriber():
    """
    indexes articles by subscribing to news aggregator output
    """
    # setup connection to database

    client = pubsub_v1.SubscriberClient()

    subscription_path = client.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

    def callback(message):
        logging.info(
            "Received message {} of message ID {}\n".format(
                message, message.message_id
            )
        )

        try:
            response = extract_body()
            logging.info(response)
            message.ack()
            logging.info("written & ack-ed")
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
    subscriber()
