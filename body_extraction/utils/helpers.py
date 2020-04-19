import concurrent.futures
import re
import logging
import requests
import time
import uuid
import urllib3
import warnings

import pandas as pd

from data.postgres_utils import connect, insert_values
from datetime import datetime
from dragnet import extract_content

logging.basicConfig(level=logging.INFO)


def warn(*args, **kwargs):
    pass


out = []
CONNECTIONS = 100
TIMEOUT = 5

warnings.warn = warn

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) \
           AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}


def do_request(url):
    try:
        requests.head(url, verify=False, timeout=10, headers=headers)
    except Exception:
        return "", 404

    try:
        res = requests.get(url, verify=False, timeout=10, headers=headers)
        content = extract_content(res.content)
        return content, res.status_code
    except Exception:
        return "", 404


def body_cleaning(text):
    """
    function to clean news content
    """
    text = re.sub('\<.*?\>', '', text)
    text = re.sub('\n', '', text)
    text = re.sub('\t', '', text)
    text = re.sub('\r', '', text)
    text = re.sub('\'s', '', text)
    text = re.sub(r'(By|by)\s\S+\s\S+', '', text)
    text = re.sub(r'https?:\/\/\S+', '', text)
    text = re.sub('www.\w+.com', '', text)
    text = re.sub('(www)?.?\w+@\w+.com', '', text)
    text = re.sub(
        '([0-9]{1,2}[\.|/|\-][0-9]{1,2}[\.|/|\-][0-9]{1,4})', '', text)
    text = re.sub(r'\xa0|\xad|\+', ' ', text)
    text = re.sub('13', '', text)
    text = re.sub('[^A-Za-z0-9,.\s]', '', text)
    return text


def gen_text_dragnet(article, timeout):
    content, status_code = do_request(article[1])
    body = body_cleaning(content[:600])
    return (article[0], body, status_code)


def extract_body():
    """
    Processing broken URLs are a huge pain in the ass
    """

    # fetch body from articles where status code is null
    query = """
                SELECT story.uuid, story.url
                FROM public.apis_story AS story
                LEFT JOIN
                (select distinct "storyID_id" from
                public.apis_storybody) AS body
                ON story.uuid = body."storyID_id"
                WHERE body."storyID_id" IS NULL
                LIMIT 10000
            """

    response = connect(query)
    if len(response) == 0:
        return True

    logging.info("extracting bodies from {} articles".format(len(response)))

    values = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
        future_to_url = (executor.submit(gen_text_dragnet, article, TIMEOUT)
                         for article in response)
        time1 = time.time()
        for future in concurrent.futures.as_completed(future_to_url):
            try:
                story_uuid, body, status = future.result()
                values.append((str(uuid.uuid4()), body, status,
                               str(datetime.now()),  story_uuid))
            except Exception as exc:
                status = str(type(exc))
            finally:
                out.append(status)
                print(str(len(out)), end="\r")

        time2 = time.time()

    query = """
            INSERT INTO public.apis_storybody
            (uuid, body, status_code, "entryTime", "storyID_id")
            VALUES(%s, %s, %s, %s, %s);
            """

    insert_values(query, values)

    logging.info(f'Took {time2-time1:.2f} s')
    logging.info(pd.Series(out).value_counts())
    return True
