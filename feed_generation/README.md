# Feed Generation

Service to generate feed from our database.


## Deploy on Cloud Function

```
gcloud functions deploy feed --runtime python37 --trigger-topic feed_generation --memory 2048 --set-env-vars DB_NAME=,DB_USER=,DB_PASSWORD=,DB_HOST=,DB_PORT= --timeout 540 --max-instances 10
```

### Popular Scenarios

risk = 'a8563fe4-f348-4a53-9c1c-07f47a5f7660'
oil = 'd3ef747b-1c3e-4582-aecb-eacee1cababe'


## Message format for triggering Cloud Function


```
gcloud pubsub topics publish feed_generation --message '{"scenario": "a8563fe4-f348-4a53-9c1c-07f47a5f7660", "mode": "portfolio"}'
```
