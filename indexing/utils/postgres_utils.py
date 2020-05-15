import json
import logging
import os
import time

from google.cloud import pubsub_v1


PROJECT_ID = os.getenv("PROJECT_ID", "alrt-ai")
TOPIC_ID = os.getenv("PUBLISHER_NAME", "insertion_test")
RESULT = False

logging.basicConfig(level=logging.INFO)


def get_callback(api_future, data, ref):
    """
    Wrap message data in the context of the callback function.
    """

    def callback(api_future):
        global RESULT

        try:
            logging.info(
                "Published message now has message ID {}".format(
                    api_future.result()
                )
            )
            ref["num_messages"] += 1
            RESULT = True
        except Exception:
            RESULT = False

            logging.info(
                "A problem occurred when publishing {}: {}\n".format(
                    data, api_future.exception()
                )
            )
            raise

    return callback


def insert_stories(stories):
    """
    insert multiple vendors into the story table
    by sending it over to the insertion service
    """

    query = """
          INSERT INTO public.apis_story
          (uuid, "entityID_id", "scenarioID_id", title, unique_hash, url,
          search_keyword, published_date, internal_source, "domain",
          "language", source_country, raw_file_source, entry_created)
          VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          ON CONFLICT DO NOTHING
          """

    payload = {}
    payload["query"] = query
    payload["data"] = stories

    payload = json.dumps(payload)

    client = pubsub_v1.PublisherClient()
    topic_path = client.topic_path(PROJECT_ID, TOPIC_ID)

    ref = dict({"num_messages": 0})

    data = str(json.dumps(payload)).encode('utf-8')

    api_future = client.publish(topic_path, data=data)
    api_future.add_done_callback(get_callback(api_future, data, ref))

    while api_future.running():
        time.sleep(0.5)
        logging.info("Published {} message(s).".format(ref["num_messages"]))

    return RESULT
