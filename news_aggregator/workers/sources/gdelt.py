import json
import requests
import sys
import logging

from datetime import datetime, timedelta
from threading import Thread

from .utils.utility import set_url

logging.basicConfig(level=logging.INFO)


class Scraper:

    DATA_URL = """https://api.gdeltproject.org/api/v2/doc/doc?query={}"""
    HISTORY_EXISTS = True
    SCRAPER_ACTIVE = True
    FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    def __init__(self, params: dict):
        self.params = params
        self.date_from = datetime.strptime(
            params["date_from"], self.FORMAT)
        self.date_to = datetime.strptime(params["date_to"], self.FORMAT)
        self.entities = params["common_names"]
        self.data_index = {}

    def fetch_data(self, *date_pairs):
        for date in date_pairs:
            self.url_filters["STARTDATETIME"] = date[0]
            self.url_filters["ENDDATETIME"] = date[1]
            url = set_url(self.url, self.url_filters)
            data = self.request_url(url)
            self.all_data.extend(data)

    def request_url(self, url):
        try:
            resp = requests.get(url=url)
            # data = resp.json()
        except requests.exceptions.Timeout:
            # retry the request
            resp = requests.get(url=url)
            # data = resp.json()
        except requests.exceptions.TooManyRedirects:
            logging.info(
                "request exceeds the configured number of maximum redirections")
        except requests.exceptions.RequestException as e:
            # catastrophic error.
            logging.info(e)
            sys.exit(1)

        try:
            data = json.loads(resp.text, strict=False)
        except Exception as e:
            logging.info("Invalid escape, ignoring response.")
            return []

        if 'articles' in data:
            return data['articles']
        else:
            return []

    def get(self):
        """
        GDELT has a cap of 250 articles per request
        so we'll do requests based on 8 hour time period
        """
        self.url_filters = {}
        self.url_filters["FORMAT"] = "json"
        self.url_filters["MAXRECORDS"] = 250

        # creating date pairs for given period
        date_pairs = []
        while self.date_from < self.date_to:
            # time pairs in 8 hours if we're searching for large difference
            # if smaller, we use 1 hour
            if (self.date_to - self.date_from).seconds//3600 >= 6:
                time_runner = self.date_from + timedelta(hours=6)
            else:
                time_runner = self.date_from + timedelta(hours=1)

            date_pairs.append(
                [self.date_from.strftime('%Y%m%d%H%M%S'), time_runner.strftime(
                    '%Y%m%d%H%M%S')])
            self.date_from = time_runner

        for entity in self.entities:
            if len(entity) < 2:
                continue

            logging.info("fetching data for keyword: {}, from {}".format(
                entity, self.params["date_from"]))
            if len(entity.split(" ")) > 1:
                query = '"{}"'.format(entity)
                self.url = self.DATA_URL.format(query)
            else:
                self.url = self.DATA_URL.format(entity)

            self.all_data = []

            for i in range(0, len(date_pairs), 200):
                logging.info("Batch: " + str(i // 200 + 1))
                threads = []

                for slide in range(i, i+200, 20):
                    # create 2 threads with 100 requests each
                    process = Thread(target=self.fetch_data,
                                     args=date_pairs[slide:slide+20])
                    process.start()
                    threads.append(process)

                for process in threads:
                    process.join()

            logging.info("{} total: {}".format(entity, len(self.all_data)))
            self.data_index[entity] = self.all_data

        return self.data_index
