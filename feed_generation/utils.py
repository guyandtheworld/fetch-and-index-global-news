import json
import logging
import math
import os
import pandas as pd
import time

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


connstr = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
connection = create_engine(connstr)


def similarity(str1, str2):
    if str1 in str2:
        return KEYWORD_SCORE
    else:
        return 0


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


def presence_score(keyword, text, analytics_type):
    """
    gives score to the article based on the presence of
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
    s = article["title_sentiment"]["compound"]

    if mode == "portfolio":
        keyword = article["search_keyword"]
    else:
        keyword = article["entity_name"]

    # negative news
    s = -s * 50

    # presence of keyword in title
    s += presence_score(keyword.lower(),
                        article["title"].lower(),
                        "title")

    # presence of keyword in body
    s += presence_score(keyword.lower(),
                        article["body"].lower(),
                        "body")

    baseScore = math.log(max(s, 1))

    timeDiff = (datetime.now() - article["published_date"]).days

    if (timeDiff >= 1):
        x = timeDiff - 1
        decayedBaseScore = baseScore * math.exp(-.01 * x * x)
    else:
        decayedBaseScore = baseScore

    scores = {"general": round(baseScore, 3),
              "general_decayed": round(decayedBaseScore, 3)}

    # generate baseScore and decayedBaseScore for buckets
    for bucket in article["buckets"]:
        bucket_id = list(bucket.keys())[0]

        # bucket score
        s += (bucket[bucket_id] * 100)

        baseScore = math.log(max(s, 1))

        timeDiff = (datetime.now() - article["published_date"]).days

        if (timeDiff >= 1):
            x = timeDiff - 1
            decayedBaseScore = baseScore * math.exp(-.01 * x * x)
        else:
            decayedBaseScore = baseScore

        scores["{}".format(bucket_id)] = round(baseScore, 3)
        scores["{}_decayed".format(bucket_id)] = round(decayedBaseScore, 3)

    return scores


def format_bucket_scores(scores):
    """
    Convert scores into bucket scores and add it to articles
    """

    story_map = {}

    scores["score_map"] = scores.apply(
        lambda x: {str(x["bucketID_id"]): round(x["grossScore"], 3)}, axis=1)

    for i in scores['storyID_id'].unique():
        temp = scores[scores['storyID_id'] == i]
        story_map[str(i)] = temp["score_map"].tolist()

    return story_map


def format_source_scores(scores):
    """
    Convert source scores and add it to articles
    """

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
    for i in range(0, len(values), 1000):
        sliced_values = values[i:i+1000]

        payload["data"] = sliced_values

        data = str(json.dumps(payload)).encode('utf-8')

        api_future = client.publish(topic_path, data=data)
        api_future.add_done_callback(get_callback(api_future, data, ref))

        while api_future.running():
            time.sleep(0.5)
            logging.info("Published {} message(s).".format(
                ref["num_messages"]))

    return RESULT
