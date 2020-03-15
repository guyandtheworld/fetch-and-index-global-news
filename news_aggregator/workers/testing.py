import sys
from aggregator import Aggregator


params = {
    "id": 45,
    "company_name": "Bharat Petroleum Corporation Limited",
    "common_names": ["BPCL", "Bharat Petroleum"],
    "source": ["gdelt"],
    "date_from": "2020-01-25T05:37:07Z",
    "date_to": "2020-01-26T05:37:07Z",
    "storage_bucket": "alrtai-testing-bucket",
}

aggregator = Aggregator(params)
aggregator.load_sources()
aggregator.write_data()
