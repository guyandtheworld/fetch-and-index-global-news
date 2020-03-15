import requests
import urllib.parse as urlparse
from urllib.parse import urlencode

params = {
    "id": 12321,
    "company_name": "Bharat Petroleum Corporation Limited",
    "common_names": ["BPCL", "Bharat Petroleum"],
    "source": ["google_news", "gdelt"],
    "date_from": "2020-01-25T05:37:07Z",
    "date_to": "2020-01-26T05:37:07Z",
    "storage_bucket": "alrtai-testing-bucket"
}

url = "http://localhost:56733/aggregate/"

url_parts = list(urlparse.urlparse(url))
query = dict(urlparse.parse_qsl(url_parts[4]))
query.update(params)

url_parts[4] = urlencode(query)

url = urlparse.urlunparse(url_parts)

response = requests.get(url)
logging.info(response.content)
