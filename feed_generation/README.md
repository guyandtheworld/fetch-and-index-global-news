# Feed Generation

Service to generate feed from our database.


## Deploy on Cloud Function

```
gcloud functions deploy feed --runtime python37 --trigger-topic feed_generation --memory 2048 --set-env-vars DB_NAME=,DB_USER=,DB_PASSWORD=,DB_HOST=,DB_PORT= --timeout 540 --max-instances 10
```
