import io
import json
import logging
import importlib.util
import os
import sys

from google.cloud import storage
from google.cloud import exceptions
from google.cloud.storage import Blob

from .publisher import publish


PROJECT_ID = os.getenv("PROJECT_ID", "alrt-ai")
SUBSCRIPTION_NAME = os.getenv("SUBSCRIPTION_NAME", "news_aggregator")


logging.basicConfig(level=logging.INFO)


class Aggregator:
    """
    synchronizes all of the scrapers based on the parameters
    that was passed by the backend

    After aggregation is complete, we write the logs
    onto a log to make sure all the data was properly
    written
    """
    sys.path.insert(1, __file__.split('aggregator')[0])

    def __init__(self, params: dict):
        self.params = params
        logging.info("parameters set")

    def load_sources(self):
        """
        Loads all the open databases we're keeping tabs on.
        """
        self.data = {}
        self.data['sources'] = {}
        logging.info("loading: " + str(self.params["source"]))

        keys = ["common_names", "date_from", "date_to"]

        source_keys = {x: self.params[x] for x in keys}

        for source in self.params["source"]:
            i = importlib.import_module("sources.{}".format(source))
            if not i.Scraper.SCRAPER_ACTIVE:
                continue
            logging.info("loading source: " + source)
            scraper = i.Scraper(source_keys)
            self.data["sources"][source] = scraper.get()

    def write_data(self):
        logging.info("writing into bucket")
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.params["storage_bucket"])

        for source in self.data["sources"].keys():
            path = "{}-{}/{}/{}-{}.json".format(self.params["entity_id"],
                                                self.params["entity_name"],
                                                source,
                                                self.params["date_from"],
                                                self.params["date_to"])

            logging.info(path)
            # create company folder
            blob = bucket.blob(path)
            blob = Blob(path, bucket)

            # write data
            f = io.StringIO(json.dumps(self.data['sources'][source]))
            try:
                blob.upload_from_file(f)
            except (AttributeError, exceptions.NotFound) as err:
                logging.error(path, self.params['storage_bucket'], err)
                raise

            # if write successful, publish to pubsub
            publish_result = publish(file_path=str(path),
                                     entity_id=str(self.params["entity_id"]),
                                     scenario_id=str(
                                         self.params["scenario_id"]),
                                     history_processed=json.dumps(
                                         self.params["history_processed"]),
                                     last_tracked=self.params["date_to"],
                                     storage_bucket=self.params["storage_bucket"],
                                     source=source
                                     )

            # ack message so we don't process it again
            if publish_result:
                logging.info("ack & write success")
                return True
