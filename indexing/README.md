# Article Indexing

Define a new Cloud function

`gcloud functions deploy index --runtime python37 --trigger-topic indexing --memory 2048 --set-env-vars DB_NAME=alrt-ai-test,DB_USER=postgres,DB_PASSWORD=alrtai2019,DB_HOST=35.247.160.130,DB_PORT=5432 --timeout 540`

See logs

`gcloud functions logs read --limit 50`
