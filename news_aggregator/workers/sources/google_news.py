import json
import os
import requests
import sys
import logging

import xml.etree.ElementTree as ET

from datetime import datetime
from threading import Thread

from .utils.utility import set_url

logging.basicConfig(level=logging.INFO)


class Scraper:
    """
    Fetch RSS news from the google news repository
    """

    DATA_URL = "https://news.google.com/news/rss/search/section/q/{}/{}"
    SCRAPER_ACTIVE = True
    FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    codes_dir = '{}/utils/codes.json'.format(os.path.dirname(__file__))
    with open(codes_dir) as json_file:
        PARAMETERS = json.load(json_file)['parameters']

    lan_dir = '{}/utils/lan_codes.json'.format(os.path.dirname(__file__))
    with open(lan_dir) as r:
        CODES = json.load(r)

    def __init__(self, params):
        self.date_from = datetime.strptime(params["date_from"], self.FORMAT)
        self.date_to = datetime.strptime(params["date_to"], self.FORMAT)

        self.entities = params["common_names"]
        self.data_index = {}

    def request_url(self, url, params):
        articles = []
        date_format = "%a, %d %b %Y %H:%M:%S %Z"

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

        root = ET.fromstring(resp.content)

        for i in root[0].iter("item"):
            article = {}

            title = i.find("title")
            if title is not None:
                article["title"] = title.text

            url = i.find("link")
            if url is not None:
                article["url"] = url.text

            pubDate = i.find("pubDate")
            if pubDate is not None:
                date = datetime.strptime(pubDate.text, date_format)
                article["pubDate"] = date

            description = i.find("description")
            if description is not None:
                article["description"] = description.text

            source = i.find("source")
            if source is not None:
                article["source"] = source.attrib["url"]

            description = i.find("description")
            if description is not None:
                article["description"] = description.text

            if params["gl"].lower() in self.CODES["country"]:
                article["country"] = self.CODES["country"][params["gl"].lower()]
            else:
                article["country"] = params["gl"]

            if params["gl"].lower() in self.CODES["language"]:
                article["language"] = self.CODES["language"][params["gl"].lower()]
            else:
                article["language"] = params["gl"]

            articles.append(article)
        return articles

    def fetch_data(self, *params):
        for param in params:
            url = set_url(self.url_structure, param)
            data = self.request_url(url, param)
            if data:
                self.all_data += data

    def get(self):
        """
        fetch data by the time
        """
        for entity in self.entities:
            if len(entity) < 2:
                continue

            logging.info("fetching data for keyword: {}".format(entity))

            self.url_structure = self.DATA_URL.format(entity, entity)

            self.all_data = []
            self.i = 1

            threads = []

            logging.info("scraping {} regions".format(len(self.PARAMETERS)))
            for slide in range(0, len(self.PARAMETERS), 5):
                process = Thread(target=self.fetch_data,
                                 args=self.PARAMETERS[slide:slide+5])
                process.start()
                threads.append(process)

            for process in threads:
                process.join()

            filtered_articles = []
            for item in self.all_data:
                if item["pubDate"] >= self.date_from and \
                        item["pubDate"] <= self.date_to:
                    item["pubDate"] = str(item["pubDate"])
                    filtered_articles.append(item)

            logging.info("{} total articles: {}".format(
                entity, len(filtered_articles)))
            self.data_index[entity] = filtered_articles

        return self.data_index
