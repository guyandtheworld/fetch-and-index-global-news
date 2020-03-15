import logging

import urllib.parse as urlparse
from urllib.parse import urlencode

logging.basicConfig(level=logging.INFO)


def set_url(data_url, url_filters):
    """
    Add filters to the url
    """
    url_parts = list(urlparse.urlparse(data_url))
    query = dict(urlparse.parse_qsl(url_parts[4]))
    query.update(url_filters)

    url_parts[4] = urlencode(query)
    url = urlparse.urlunparse(url_parts)
    return url
