import logging
import math
import os
import pandas as pd

from datetime import datetime
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)


params = {
    'database': os.environ["DB_NAME"],
    'user': os.environ["DB_USER"],
    'password': os.environ["DB_PASSWORD"],
    'host': os.environ["DB_HOST"],
    'port': os.environ["DB_PORT"],
}


conn_str = f'postgresql://{params["user"]}:{params["password"]}@{params["host"]}:{params["port"]}/{params["database"]}'
connection = create_engine(conn_str)


KEYWORD_SCORE = 20


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


def hotness(article, bucket, mode, score_type):
    """
    Adding to score if the company term is in title
    * domain score - domain reliability
    """
    s = article["title_sentiment"]["compound"]

    if mode == "portfolio":
        keyword = article["search_keyword"]
    else:
        keyword = article["name"]

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

    if bucket:
        # bucket score + source score
        s += (article["grossScore"] * 100)

    baseScore = math.log(max(s, 1))

    timeDiff = (datetime.now() - article["published_date"]).days

    if (timeDiff >= 1):
        x = timeDiff - 1
        decayedBaseScore = baseScore * math.exp(-.01 * x * x)

    scores = {"{}": round(baseScore, 3),
              "{}_decayed": round(decayedBaseScore, 3)}

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
