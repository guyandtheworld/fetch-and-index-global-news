import json
import logging
import math
import os
import pandas as pd
import time
import psycopg2
import re

from google.cloud import pubsub_v1
from datetime import datetime
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)

RESULT = False
PROJECT_ID = os.getenv("PROJECT_ID", "alrt-ai")
TOPIC_ID = os.getenv("PUBLISHER_NAME", "insertion_test")
KEYWORD_SCORE = 20

# db credentials
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]

params = {
    'database': os.environ["DB_NAME"],
    'user': os.environ["DB_USER"],
    'password': os.environ["DB_PASSWORD"],
    'host': os.environ["DB_HOST"],
    'port': os.environ["DB_PORT"],
}

connstr = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
connection = create_engine(connstr)


def select(query):
    """
    Connect to the PostgreSQL database server
    """
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    logging.info('running : {}'.format(query))
    cur.execute(query)
    results = cur.fetchone()
    cur.close()
    return results


def delete_feed(query):
    """
    Delete feed if new model exists
    """
    conn = psycopg2.connect(**params)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()


def add_to_dataframe(articles, query, name=None):
    """
    Fetch entities of an article and insert that into
    the Dataframe.
    """
    ids_str = "', '".join(articles["uuid"].apply(str).values)
    ids_str = "('{}')".format(ids_str)

    df = pd.read_sql(query.format(ids_str), connection)

    articles = articles.merge(df, how='left',
                              left_on="uuid", right_on="storyID_id")

    articles = articles.dropna()
    logging.info("fetched {} {}".format(articles.shape[0], name))
    articles.drop('storyID_id', axis=1, inplace=True)
    return articles


def generate_story_entities(entities):
    """
    Take in a list of 'processed' stories
    and return the entities found in those stories
    """
    if len(entities) == 0:
        return entities

    entities = entities.astype(str)
    entities = entities.rename({"entityID_id": "entity_id"}, axis=1)

    story_map = {}

    for i in entities['storyID_id'].unique():
        temp = entities[entities['storyID_id'] == i]
        story_map[i] = temp.drop(['storyID_id'], axis=1).to_dict('records')

    return story_map


def similarity(keyword, text):
    """
    Generates score based on the number of times keywords
    appear on the text body
    """
    count = sum(1 for _ in re.finditer(r'\b%s\b' % re.escape(keyword), text))

    if len(keyword) < 4 and count > 2:
        return 30

    return count * KEYWORD_SCORE


def presence_score(keyword, text, analytics_type):
    """
    Gives score to the article based on the presence of
    relevant keyword in the content and the body
    """
    score = similarity(keyword, text)

    if analytics_type == 'title':
        return score * 2
    else:
        return score


def hotness(article, mode):
    """
    Adding to score if the company term is in title
    * domain score - domain reliability
    """
    title_sentiment = article["title_sentiment"]["compound"]
    body_sentiment = article["body_sentiment"]["compound"]

    s = (title_sentiment + body_sentiment) / 2

    if mode == "portfolio":
        keyword = article["search_keyword"]
    else:
        keyword = article["entity_name"]

    # negative news
    s = -s * 50 + 50

    # presence of keyword in title
    s += presence_score(keyword.lower(), article["title"].lower(),
                        "title")

    # presence of keyword in body
    s += presence_score(keyword.lower(), article["body"].lower(),
                        "body")

    baseScore = math.log(max(s, 1))
    scores = {"general": round(baseScore, 3)}

    # generate baseScore and decayedBaseScore for buckets
    for bucket_id, bucket_score in article["buckets"].items():

        # bucket score
        s += (bucket_score * 50)

        baseScore = math.log(max(s, 1))
        scores["{}".format(bucket_id)] = round(baseScore, 3)

    return scores


def format_bucket_scores(scores):
    """
    Convert scores into bucket scores and add it to articles
    """

    if scores.shape[0] == 0:
        return {}

    story_map = {}

    scores["score_map"] = scores.apply(
        lambda x: [str(x["bucketID_id"]), round(x["grossScore"], 3)], axis=1)

    for i in scores['storyID_id'].unique():
        rows = scores[scores['storyID_id'] == i]
        story_scores = rows["score_map"].tolist()
        score_map = {score[0]: score[1] for score in story_scores}
        story_map[str(i)] = score_map

    return story_map


def format_source_scores(scores):
    """
    Convert source scores and add it to articles
    """

    if scores.shape[0] == 0:
        return {}

    story_map = {}

    for i in scores['storyID_id'].unique():
        temp = scores[scores['storyID_id'] == i]
        story_map[str(i)] = temp["sourceScore"].iloc[0]

    return story_map


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


def insert_values(query, values):
    """
    Use pub-sub to insert the values into CloudSQL database
    """
    payload = {}
    payload["query"] = query
    payload["source"] = "feed_generation"

    client = pubsub_v1.PublisherClient()
    topic_path = client.topic_path(PROJECT_ID, TOPIC_ID)

    ref = dict({"num_messages": 0})

    # deliver only maximum of 1000 stories at once
    for i in range(0, len(values), 500):
        sliced_values = values[i:i+500]

        payload["data"] = sliced_values

        data = str(json.dumps(payload)).encode('utf-8')

        api_future = client.publish(topic_path, data=data)
        api_future.add_done_callback(get_callback(api_future, data, ref))

        while api_future.running():
            time.sleep(0.5)
            logging.info("Published {} message(s).".format(
                ref["num_messages"]))

    return RESULT
