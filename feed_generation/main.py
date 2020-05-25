import logging
import pandas as pd

from utils import connection


logging.basicConfig(level=logging.INFO)


def join_body(articles):
    """
    Add body to the articles scraped, remove rows without body
    """

    ids_str = "', '".join(articles["uuid"].apply(str).values)
    ids_str = "('{}')".format(ids_str)

    query = """
            SELECT "storyID_id", (array_agg(body))[1] AS body
            FROM apis_storybody
            WHERE "storyID_id" in {}
            GROUP BY "storyID_id"
            """.format(ids_str)

    body = pd.read_sql(query, connection)
    logging.info("fetching body: {}".format(body.shape[0]))

    articles = articles.merge(body, how='left',
                              left_on="uuid", right_on="storyID_id")

    articles = articles[articles.notna()]
    logging.info("articles: {}".format(articles.shape[0]))
    return articles


def generate_feed():
    """
    Take new stories and generate the feed out of it.
    """

    query = """
            SELECT uuid, title, unique_hash, url, search_keyword,
            published_date, "domain", "language", source_country,
            "entityID_id", "scenarioID_id"
            FROM public.apis_story WHERE uuid NOT IN
            (SELECT story_id FROM feed_storywarehouse)
            LIMIT 10
            """
    articles = pd.read_sql(query, connection)
    logging.info("articles: {}".format(articles.shape[0]))

    articles = join_body(articles)
    print(articles)
    # join_sentiment()
    # join_entities()
    # join_cluster()
    # join_scores()
    # join_hotness()


generate_feed()
