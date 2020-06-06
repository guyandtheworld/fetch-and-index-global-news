# Article Indexing

Deploy on Cloud Function

`gcloud functions deploy index --runtime python37 --trigger-topic indexing --memory 2048 --set-env-vars DB_NAME=,DB_USER=,DB_PASSWORD=,DB_HOST=,DB_PORT= --timeout 540`

### See logs

`gcloud functions logs read --limit 50`


## Testing

### Testing Locally

* call function `test_index()`
* add above params to the params
* comment out `resp = insert_stories(results)`
* run `python main.py -m test_index()`

### Trigger function

`gcloud pubsub topics publish indexing_test --message '{"source_file": "dd61a22b-b9a3-475c-8581-474411b17898-Naptha Storage/google_news/2020-04-21T07:47:08Z-2020-04-22T12:16:41Z.json",
"entity_id": "dd61a22b-b9a3-475c-8581-474411b17898",
"scenario_id": "d3ef747b-1c3e-4582-aecb-eacee1cababe",
"history_processed": "true",
"last_tracked": "2020-04-22T12:16:41Z",
"storage_bucket": "news_staging_bucket",
"source": "google_news"}'
`

{'source_file': 'e008bcc4-d063-433b-9c16-1bfe2d581fd7-Renewable Energy/gdelt/2020-06-05T15:21:12Z-2020-06-05T21:08:08Z.json', 'entity_id': 'e008bcc4-d063-433b-9c16-1bfe2d581fd7',
'scenario_id': '0edd503a-810f-421e-b3cd-da9e506c3596', 'history_processed': 'true', 'last_tracked': '2020-06-05T21:08:08Z', 'storage_bucket': 'news_staging_bucket', 'source': 'gdelt'}