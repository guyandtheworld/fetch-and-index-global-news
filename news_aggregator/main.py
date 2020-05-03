#!/usr/bin/env python
import base64
import logging
import json

from workers.aggregator import Aggregator


logging.basicConfig(level=logging.INFO)


def test_aggregate():
    """
    Function to test if news aggregator works
    """
    params = {"entity_id": "f36ca121-8dca-4bbe-9f48-868b07e34b83",
              "entity_name": "Naphtha transportation",
              "common_names": ["Naphtha transportation"],
              "scenario_id": "d3ef747b-1c3e-4582-aecb-eacee1cababe",
              "source": ["gdelt"],
              "date_from": "2019-04-29T05:22:25Z",
              "date_to": "2020-04-29T06:41:08Z",
              "storage_bucket": "news_staging_bucket",
              "history_processed": True,
              "write": True}

    aggregator = Aggregator(params)
    aggregator.load_sources()
    if "write" not in params:
        aggregator.write_data()


def news_aggregator(event, context):
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
            logging.info("starting aggregation")
            aggregator = Aggregator(params)
            aggregator.load_sources()
            if "write" not in params:
                aggregator.write_data()
        except Exception as e:
            logging.info(
                "message processing failed. up for retry. - " + str(e))
    else:
        logging.info("message format broken")
