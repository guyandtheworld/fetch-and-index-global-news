import hashlib
import uuid
import logging

from datetime import datetime
from langdetect import detect


logging.basicConfig(level=logging.INFO)


def isEnglish(text):
    try:
        lang = detect(text)
    except Exception as e:
        logging.info(e)
        logging.info(text)
        return False

    if lang == 'en':
        return True
    else:
        return False


def gdelt(data, entity_id, scenario_id, source_file):
    """
    first item on the list is the common name
    second item is the ticker. should be updated
    when aliases are added
    """
    date_format = "%Y%m%dT%H%M%SZ"
    articles = []
    for key in data.keys():
        for article in data[key]:
            unique_hash = hashlib.md5(article['url'].encode()).hexdigest()
            pub_date = datetime.strptime(article["seendate"], date_format)

            # if language is not english, don't index
            if not isEnglish(article["title"]):
                continue

            article = (
                str(uuid.uuid4()),
                entity_id,
                scenario_id,
                article["title"],
                unique_hash,
                article["url"],
                key,
                str(pub_date),
                "gdelt",
                article["domain"],
                article["language"].lower(),
                article["sourcecountry"].lower(),
                source_file,
                str(datetime.utcnow()),)

            articles.append(article)
    return articles


def google_news(data, entity_id, scenario_id, source_file):
    """
    format for converting google news data
    into our model
    """
    date_format = "%Y-%m-%d %H:%M:%S"
    articles = []
    for key in data.keys():
        for article in data[key]:

            # if language is not english, don't index
            if not isEnglish(article["title"]):
                continue

            unique_hash = hashlib.md5(article['url'].encode()).hexdigest()
            pub_date = datetime.strptime(article["pubDate"], date_format)
            article = (
                str(uuid.uuid4()),
                entity_id,
                scenario_id,
                article["title"],
                unique_hash,
                article["url"],
                key,
                str(pub_date),
                "google_news",
                article["source"],
                article["language"],
                article["country"],
                source_file,
                str(datetime.utcnow()),)

            articles.append(article)
    return articles
