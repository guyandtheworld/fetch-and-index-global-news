import json
import hashlib
import responses

from datetime import datetime, timedelta
from workers.sources.GDELT import Scraper


class TestWorkers:

    @responses.activate
    def test_aggregate_endpoint(self):
        with open('tests/data/data.json') as json_file:
            mock_data = json.load(json_file, strict=False)

        date = datetime.today()
        url = "https://api.gdeltproject.org/api/v2/doc/doc?query=robinhood&FORMAT=json&MAXRECORDS=250&STARTDATETIME={}&ENDDATETIME={}"
        resp = mock_data[url]
        for _ in range(3):
            prev_date = date - timedelta(hours=8)
            uri = url.format(prev_date.strftime('%Y%m%d%H%M%S'),
                             date.strftime('%Y%m%d%H%M%S'))
            logging.info(uri)
            responses.add(responses.GET, uri, json=resp, status=200)
            date = prev_date

        test_params = {
            "entity": "robinhood",
            "is_new_entity": False,
            "entity_type": "company",
            "region": None,
            "language": None,
            "date_start": None,
            "date_end": None
        }

        scraper = Scraper(test_params)
        scraper.daily()
        hash_calculated = hashlib.md5(
            resp['articles'][0]['url'].encode()).hexdigest()
        assert(scraper.final["articles"][0]["hash"] == hash_calculated)
