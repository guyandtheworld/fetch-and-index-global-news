# Body Extraction

Deploy on Cloud Function

`gcloud functions deploy body_extraction --runtime python37 --trigger-topic extract_body_test --memory 2048 --set-env-vars DB_NAME=,DB_USER=,DB_PASSWORD=,DB_HOST=,DB_PORT= --timeout 540`

### See logs

`gcloud functions logs read --limit 50`


## Testing

### Testing Locally

* call function `test_body_extraction`
* add above params to the params
* comment out `insert_stories`
* run `python main.py -m test_index()`

### Trigger function

`gcloud pubsub topics publish indexing_test --message random`
