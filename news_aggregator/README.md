# News Aggregator

## Deploy on Cloud Function

`gcloud functions deploy news_aggregator --runtime python37 --trigger-topic news_aggregator_test --memory 2048 --set-env-vars DB_NAME=,DB_USER=,DB_PASSWORD=,DB_HOST=,DB_PORT= --timeout 540`

## Sample Publish Data Format

```
gcloud pubsub topics publish news_aggregator_test --message '{
    "entity_id": "f36ca121-8dca-4bbe-9f48-868b07e34b83",
    "entity_name": "Naphtha transportation",
    "common_names": ["Naphtha transportation"],
    "scenario_id": "d3ef747b-1c3e-4582-aecb-eacee1cababe",
    "source": ["gdelt"],
    "date_from": "2019-04-29T05:22:25Z",
    "date_to": "2020-04-29T06:41:08Z",
    "storage_bucket": "news_staging_bucket",
    "history_processed": True,
    "write": True
}'
```


## Testing locally

```
python -c "from main import test_aggregate; test_aggregate()"
```
