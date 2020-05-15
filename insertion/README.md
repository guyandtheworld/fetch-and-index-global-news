# Insertion Service

Service which is used as a global insertion for all of the services using pub-sub.

```
gcloud functions deploy insertion --runtime python37 --trigger-topic insertion_test --set-env-vars DB_NAME=,DB_USER=,DB_PASSWORD=,DB_HOST=,DB_PORT= --timeout 540 --max-instances 1
```
