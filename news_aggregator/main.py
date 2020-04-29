#!/usr/bin/env python
import base64
import logging
import json

from workers.aggregation_worker import start_aggregation


logging.basicConfig(level=logging.INFO)


def test_aggregate():
    params = {'path': 'dd61a22b-b9a3-475c-8581-474411b17898-Naptha Storage/google_news/2020-04-21T07:47:08Z-2020-04-22T12:16:41Z.json',
              'entity_id': 'dd61a22b-b9a3-475c-8581-474411b17898',
              'scenario_id': 'd3ef747b-1c3e-4582-aecb-eacee1cababe',
              'history_processed': 'true',
              'last_tracked': '2020-04-22T12:16:41Z',
              'storage_bucket': 'news_staging_bucket',
              'source': 'google_news'}
    print(json.dumps(params))

    # start_aggregation(params)


def aggregate(event, context):
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

    if 'data' in event:
        str_params = base64.b64decode(event['data']).decode('utf-8')
        params = json.loads(str_params)
    else:
        logging.info("no text in the message")
        return

    if params:
        try:
            start_aggregation(params)
        except Exception as e:
            logging.info(
                "message processing failed. up for retry. - " + str(e))
    else:
        logging.info("message format broken")
