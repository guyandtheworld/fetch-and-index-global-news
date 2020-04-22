import logging
import ndjson
import pandas as pd

from . import source_processor


logging.basicConfig(level=logging.INFO)


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

    # convert to string
    json_data_string = blob.download_as_string()
    data = ndjson.loads(json_data_string)

    processor = getattr(source_processor, record["source"])
    logging.info("processing: {}".format(record["source_file"]))
    processed_records = processor(data, record["entity_id"],
                                  record["scenario_id"],
                                  record["source_file"])

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
