import logging
import pandas as pd
import numpy as np

from utils import (connection,
                   add_to_dataframe,
                   generate_story_entities)


logging.basicConfig(level=logging.INFO)


def join_body(articles):
    """
    Add body to the articles scraped, remove rows without body
    """
    query = """
            SELECT "storyID_id", (array_agg(body))[1] AS body
            FROM apis_storybody
            WHERE "storyID_id" in {}
            GROUP BY "storyID_id"
            """

    return add_to_dataframe(articles, query)


def join_sentiment(articles, title=True):
    """
    Add sentiment to the articles scraped, remove rows without sentiment
    """

    if title:
        query = """
                select "storyID_id", (array_agg(sentiment))[1] as title_sentiment
                from apis_storysentiment WHERE "storyID_id" in {}
                AND is_headline = false GROUP BY "storyID_id"
                """
    else:
        query = """
                select "storyID_id", (array_agg(sentiment))[1] as body_sentiment
                from apis_storysentiment WHERE "storyID_id" in {}
                AND is_headline = false GROUP BY "storyID_id"
                """

    return add_to_dataframe(articles, query)


def join_cluster(articles):
    """
    Add body to the articles scraped, remove rows without body
    """

    query = """
            select "storyID_id", (array_agg(cluster))[1] AS cluster
            from ml_clustermap clustermap inner join
            ml_cluster clust on clustermap."clusterID_id" = clust.uuid
            WHERE "storyID_id" in {}
            GROUP BY "storyID_id"
            """

    return add_to_dataframe(articles, query)


def join_entities(articles):
    """
    Add entities to the articles scraped, remove rows without entities
    """

    query = """
            SELECT entitymap.uuid, "storyID_id",
            enref.name, entype.name as type
            FROM apis_storyentitymap AS entitymap
            INNER JOIN apis_storyentityref AS enref
            ON entitymap."entityID_id" = enref.uuid
            INNER JOIN apis_entitytype as entype
            ON enref."typeID_id" = entype.uuid
            WHERE "storyID_id" in {}
            """

    ids_str = "', '".join(articles["uuid"].apply(str).values)
    ids_str = "('{}')".format(ids_str)

    entities = pd.read_sql(query.format(ids_str), connection)

    story_map = generate_story_entities(entities)

    articles["uuid"] = articles["uuid"].astype(str)

    entities_column = []
    for i, row in articles.iterrows():
        if row['uuid'] in story_map:
            entities_column.append(story_map[row['uuid']])
        else:
            entities_column.append([])

    articles["entities"] = entities_column
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
    articles = join_sentiment(articles, title=True)
    articles = join_sentiment(articles, title=False)
    articles = join_cluster(articles)
    articles = join_entities(articles)
    # join_scores()
    # join_hotness()


generate_feed()
