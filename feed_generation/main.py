import logging
import pandas as pd
from itertools import chain

from utils import (add_to_dataframe,
                   connection,
                   format_bucket_scores,
                   format_source_scores,
                   generate_story_entities,
                   hotness)


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

    return add_to_dataframe(articles, query, name="body")


def join_sentiment(articles, title=True):
    """
    Add sentiment to the articles scraped, remove rows without sentiment
    """

    if title:
        query = """
                select "storyID_id", (array_agg(sentiment))[1]
                as title_sentiment from apis_storysentiment
                WHERE "storyID_id" in {} AND is_headline = true
                GROUP BY "storyID_id"
                """
    else:
        query = """
                select "storyID_id", (array_agg(sentiment))[1]
                as body_sentiment from apis_storysentiment
                WHERE "storyID_id" in {} AND is_headline = false
                GROUP BY "storyID_id"
                """

    return add_to_dataframe(articles, query, name="sentiment")


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

    return add_to_dataframe(articles, query, name="cluster")


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
    for _, row in articles.iterrows():
        if row['uuid'] in story_map:
            entities_column.append(story_map[row['uuid']])
        else:
            entities_column.append([])

    articles["entities"] = entities_column
    return articles


def join_bucket_scores(articles, scenario):
    """
    Fetch the bucket scores from the latest model
    and add it to the story
    """

    query = """
            select uuid from apis_modeldetail am2 where "version" =
            (select max(version) from apis_modeldetail am
            where "scenarioID_id" = '{}')
            and "scenarioID_id" = '{}'
            """

    model_id = pd.read_sql(query.format(scenario, scenario), connection)

    query = """
            SELECT "storyID_id", "bucketID_id", "grossScore",
            src.score as "sourceScore"
            FROM apis_bucketscore scores INNER JOIN
            (SELECT * FROM apis_source) src
            ON src.uuid = scores."sourceID_id"
            INNER JOIN apis_bucket bucket
            ON scores."bucketID_id" = bucket.uuid
            WHERE "modelID_id" = '{}'
            AND "storyID_id" IN {}
            """

    ids_str = "', '".join(articles["uuid"].apply(str).values)
    ids_str = "('{}')".format(ids_str)

    scores = pd.read_sql(query.format(
        str(model_id.iloc[0]["uuid"]), ids_str), connection)

    scores.drop_duplicates(['storyID_id', 'bucketID_id'],
                           keep='last', inplace=True)

    logging.info("fetched {} {}".format(scores.shape[0], "score"))

    bucket_scores = format_bucket_scores(scores)
    source_scores = format_source_scores(scores)

    # adding the bucket scores to the dataframe
    buckets_column = []
    for _, row in articles.iterrows():
        if row['uuid'] in bucket_scores:
            buckets_column.append(bucket_scores[row['uuid']])
        else:
            buckets_column.append([])

    articles["buckets"] = buckets_column

    # add source scores to the dataframe
    articles["scores"] = articles["uuid"].apply(
        lambda x: {"source_scores": source_scores[str(x)]})

    return articles


def generate_hotness(articles):
    """
    Generate different types of score for articles
    """

    articles["published_date"] = articles["published_date"].dt.tz_localize(
        None)

    # generate general hotness
    articles["hotness"] = articles.apply(lambda x: hotness(
        x, mode="portfolio"), axis=1)

    return articles


def insertion_cleaning(articles):
    """
    Clean the dataframe to have proper format while
    Insertion
    """
    # merge sentiment

    def merge_sentiment(row):

        merged = {value: round((row["title_sentiment"][value] +
                                row["body_sentiment"][value])/2, 3)
                  for value in types}
        return merged

    types = ["neg", "neu", "pos", "compound"]
    articles["sentiment"] = articles[["title_sentiment",
                                      "body_sentiment"]].apply(merge_sentiment,
                                                               axis=1)
    articles.drop(["title_sentiment", "body_sentiment"], axis=1, inplace=True)
    return articles


def insert_values():
    query = """
            INSERT INTO public.feed_storywarehouse
            (uuid, story_id, story_title, story_url, published_date,
            "domain", "language", source_country, entity_name,
            entity_id, scenario_id, story_body, "timestamp",
            scores, entities, hotness, bucket_score,
            sentiment, "cluster")
            VALUES(?, ?, '', '', '', '', '', '', '', ?,
            ?, '', '', '', '', '', '', '', 0);
            """


def generate_feed():
    """
    Take new stories and generate the feed out of it.
    * Only run decay if days < 30
    """

    SCENARIO = 'a8563fe4-f348-4a53-9c1c-07f47a5f7660'

    # filter by published date descending
    # if new model, clean the existing table
    query = """
            SELECT uuid, title, url, search_keyword,
            published_date, "domain", "language", source_country,
            "entityID_id", "scenarioID_id"
            FROM public.apis_story WHERE uuid NOT IN
            (SELECT story_id FROM feed_storywarehouse)
            AND "scenarioID_id" = '{}'
            LIMIT 10
            """.format(SCENARIO)

    articles = pd.read_sql(query, connection)
    logging.info("articles: {}".format(articles.shape[0]))

    if len(articles) == 0:
        return

    articles = join_body(articles)
    articles = join_sentiment(articles, title=True)
    articles = join_sentiment(articles, title=False)
    articles = join_cluster(articles)
    articles = join_entities(articles)
    articles = join_bucket_scores(articles, SCENARIO)
    articles = generate_hotness(articles)

    articles = insertion_cleaning(articles)


generate_feed()
