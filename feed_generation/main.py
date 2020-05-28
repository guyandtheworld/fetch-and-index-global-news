import logging
import pandas as pd

from utils import (add_to_dataframe,
                   connection,
                   format_bucket_scores,
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


def join_hotness(articles, bucket=False, sentiment=True, mode="portfolio"):
    """
    Generate different types of score for articles
    """

    articles["published_date"] = articles["published_date"].dt.tz_localize(
        None)
    articles["hotness"] = articles.apply(lambda x: hotness(
        x, bucket, sentiment, mode), axis=1)

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
            SELECT "storyID_id", "bucketID_id", "grossScore"
            FROM apis_bucketscore scores
            INNER JOIN
            apis_bucket bucket
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

    buckets_column = []
    for _, row in articles.iterrows():
        if row['uuid'] in bucket_scores:
            buckets_column.append(bucket_scores[row['uuid']])
        else:
            buckets_column.append([])

    articles["buckets"] = buckets_column

    return articles


def generate_feed():
    """
    Take new stories and generate the feed out of it.
    """

    SCENARIO = 'a8563fe4-f348-4a53-9c1c-07f47a5f7660'

    # filter by published date descending
    query = """
            SELECT uuid, title, unique_hash, url, search_keyword,
            published_date, "domain", "language", source_country,
            "entityID_id", "scenarioID_id"
            FROM public.apis_story WHERE uuid NOT IN
            (SELECT story_id FROM feed_storywarehouse)
            AND "scenarioID_id" = '{}'
            LIMIT 10
            """.format(SCENARIO)

    articles = pd.read_sql(query, connection)
    logging.info("articles: {}".format(articles.shape[0]))

    articles = join_body(articles)
    articles = join_sentiment(articles, title=True)
    articles = join_sentiment(articles, title=False)
    articles = join_cluster(articles)
    articles = join_entities(articles)
    articles = join_hotness(articles)
    articles = join_bucket_scores(articles, SCENARIO)

    print(articles.columns)


generate_feed()
