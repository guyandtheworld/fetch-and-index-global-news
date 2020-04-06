# Entity Extraction

Define a new Cloud function

`gcloud functions deploy extract --runtime python37 --trigger-topic extract_entities`

Publish a message to pub-sub

`gcloud pubsub topics publish extract_entities --message Adarsh`

Read logs of the Cloud function

`gcloud functions logs read --limit 50`
