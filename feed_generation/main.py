import json
import logging
import pandas as pd
import uuid

from datetime import datetime
from utils import (add_to_dataframe,
                   connection,
                   format_bucket_scores,
                   format_source_scores,
                   generate_story_entities,
                   hotness,
                   insert_values)


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

    def format_entities(values):
        """
        Format entities in a way that's easy to render in the
        frontend.
        """
        final = {}

        for value in values:
            if value["type"] in final:
                final[value["type"]][value["name"]] = value["uuid"]
            else:
                final[value["type"]] = {value["name"]: value["uuid"]}
        return final

    articles["entities"] = articles["entities"].apply(format_entities)

    drop_rows = articles[articles["entities"].map(len) == 0].index
    logging.info("Dropping {} rows based on entities".format(len(drop_rows)))
    articles = articles.drop(drop_rows)

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

    drop_rows = articles[articles["buckets"].map(len) == 0].index
    logging.info("Dropping {} rows based on score".format(len(drop_rows)))
    articles = articles.drop(drop_rows)

    logging.info("Joining scores")

    # add source scores to the dataframe
    articles["scores"] = articles["uuid"].apply(
        lambda x: {"source_scores": source_scores[str(x)]})

    return articles


def generate_hotness(articles, mode):
    """
    Generate different types of score for articles
    """

    articles["published_date"] = articles["published_date"].dt.tz_localize(
        None)

    logging.info("Generating hotness")

    # generate general hotness
    articles["hotness"] = articles.apply(lambda x: hotness(
        x, mode), axis=1)

    return articles


def insertion_cleaning(articles):
    """
    Clean the dataframe to have proper format while
    Insertion
    """

    logging.info("Cleaning the feed data")

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

    articles = articles.dropna()

    # rename columns
    articles.rename(columns={'uuid': 'storyID'}, inplace=True)
    articles["timestamp"] = str(datetime.now())
    articles["published_date"] = articles["published_date"].apply(str)
    articles["storyID"] = articles["storyID"].apply(str)
    articles["entityID_id"] = articles["entityID_id"].apply(str)
    articles["scenarioID_id"] = articles["scenarioID_id"].apply(str)

    def jsonify(x): return json.dumps(x)

    articles["scores"] = articles["scores"].apply(jsonify)
    articles["entities"] = articles["entities"].apply(jsonify)
    articles["hotness"] = articles["hotness"].apply(jsonify)
    articles["buckets"] = articles["buckets"].apply(jsonify)
    articles["sentiment"] = articles["sentiment"].apply(jsonify)

    articles['uuid'] = [str(uuid.uuid4()) for _ in range(len(articles.index))]

    # reorder articles
    articles = articles[['uuid', 'storyID', 'title', 'url', 'published_date',
                         'domain', 'language', 'source_country',
                         'search_keyword', 'entity_name', 'entityID_id',
                         'scenarioID_id', 'body', 'timestamp', 'cluster',
                         'scores', 'entities', 'hotness', 'buckets',
                         'sentiment']]

    return articles


def generate_feed(scenario, mode):
    """
    Take new stories and generate the feed out of it.
    * Only run decay if days < 30
    """

    # filter by published date descending
    # if new model, clean the existing table
    if mode == "auto":
        query = """
                SELECT story.uuid, title, url, search_keyword,
                published_date, "domain", "language", source_country,
                entityref.name as "entity_name", storymap."entityID_id",
                story."scenarioID_id" FROM
                apis_storyentitymap storymap
                INNER JOIN apis_story story
                on storymap."storyID_id" = story.uuid
                INNER JOIN apis_storyentityref entityref
                ON story."entityID_id" = entityref.uuid
                WHERE story.uuid NOT IN
                (SELECT "storyID" FROM feed_autowarehouse)
                AND story."scenarioID_id" = '{}'
                ORDER BY story.published_date DESC
                LIMIT 10
                """.format(scenario)
    elif mode == "portfolio":
        query = """
                SELECT story.uuid, title, url, search_keyword,
                published_date, "domain", "language", source_country,
                entity.name as "entity_name", "entityID_id",
                story."scenarioID_id" FROM public.apis_story story
                INNER JOIN public.apis_entity entity
                ON story."entityID_id" = entity.uuid
                WHERE story.uuid NOT IN
                (SELECT "storyID" FROM feed_portfoliowarehouse)
                AND story."scenarioID_id" = '{}'
                ORDER BY story.published_date DESC
                LIMIT 10
                """.format(scenario)
    else:
        return None

    articles = pd.read_sql(query, connection)
    logging.info("articles: {}".format(articles.shape[0]))

    if len(articles) == 0:
        return

    articles = join_body(articles)

    if len(articles) == 0:
        return False

    articles = join_sentiment(articles, title=True)

    if len(articles) == 0:
        return False

    articles = join_sentiment(articles, title=False)

    if len(articles) == 0:
        return False

    articles = join_cluster(articles)

    if len(articles) == 0:
        return False

    articles = join_entities(articles)

    if len(articles) == 0:
        return False

    articles = join_bucket_scores(articles, scenario)

    if len(articles) == 0:
        return False

    articles = generate_hotness(articles, mode)
    articles = insertion_cleaning(articles)

    if mode == "auto":
        query = """
                INSERT INTO public.feed_autowarehouse
                (uuid, "storyID", title, url, published_date, "domain",
                "language", source_country, search_keyword, entity_name,
                "entityID", "scenarioID", story_body, "timestamp", "cluster",
                scores, entities, hotness, bucket_scores, sentiment)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s);
                """
    elif mode == "portfolio":
        query = """
                INSERT INTO public.feed_portfoliowarehouse
                (uuid, "storyID", title, url, published_date, "domain",
                "language", source_country, search_keyword, entity_name,
                "entityID", "scenarioID", story_body, "timestamp", "cluster",
                scores, entities, hotness, bucket_scores, sentiment)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s);
                """
    else:
        return None

    logging.info("Inserting {} data".format(articles.shape[0]))

    values = [tuple(row) for row in articles.itertuples(index=False)]
    insert_values(query, values)


def test_feed():
    """
    Mode:
    * Auto
    * Portfolio

    Type:
    * Hot news
    * Historic
    """

    risk = 'a8563fe4-f348-4a53-9c1c-07f47a5f7660'
    oil = 'd3ef747b-1c3e-4582-aecb-eacee1cababe'

    generate_feed(oil, mode="auto")


test_feed()
