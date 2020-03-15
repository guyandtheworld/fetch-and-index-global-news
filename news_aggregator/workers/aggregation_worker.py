import logging

from .aggregator import Aggregator


logging.basicConfig(level=logging.INFO)


def start_aggregation(params):
    """
    Call the Scraper with the parameters passed by the server
    """
    logging.info("starting aggregation")
    aggregator = Aggregator(params)
    aggregator.load_sources()
    result = aggregator.write_data()
    return result
