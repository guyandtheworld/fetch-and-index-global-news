import hashlib
import logging
import json
import os
import pandas as pd

from data import source_processor


logging.basicConfig(level=logging.INFO)

DESTINATION_FOLDER = "temp"


def index_articles(record: dict):
    """
    depending on the source the article is from
    we pre-process the json and write it onto
    the Article object and save it

    ## args
    * record: a remote raw json file storage
    * metadata: details regarding the company and the source

    ## returns
    processed articles based on MongoDB Article model
    """
    with open(record["file"], "r") as fp:
        data = json.load(fp)
        processor = getattr(source_processor, record["source"])
        logging.info("processing: {}".format(record["source_file"]))
        processed_records = processor(data, record["entity_id"],
                                      record["scenario_id"],
                                      record["source_file"])
    return processed_records


def process_company_json(record: dict, bucket):
    """
    fetches file and stores it locally to fetch and preprocess
    returns the processed articles

    ## args
    * record: a remote raw json file storage
    * bucket: Google Bucket Instance
    * metadata: details regarding the company and the source

    ## returns
    processed articles based on MongoDB Article model
    """

    blob = bucket.blob(record["source_file"])

    # naming temp file
    hash_f = hashlib.sha1(
        str(record["source_file"]).encode("UTF-8")).hexdigest()
    file_path = "{}/{}.json".format(DESTINATION_FOLDER, hash_f)

    blob.download_to_filename(file_path)
    record["file"] = file_path
    processed_records = index_articles(record)
    os.remove(file_path)

    # delete duplicates here
    df = pd.DataFrame(processed_records,
                      columns=["uuid", "entityID_id", "scenarioID_id",
                               "title", "unique_hash", "url",
                               "search_keyword", "published_date",
                               "internal_source", "domain",
                               "language", "source_country",
                               "raw_file_source", "entry_created"])

    before = df.shape
    df.drop_duplicates(subset='url', keep="first", inplace=True)
    after = df.shape
    logging.info("Before: {}, After: {}".format(before, after))

    processed_records = list(df.to_records(index=False))
    return processed_records
