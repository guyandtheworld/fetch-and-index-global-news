# Body Extraction

Deploy on Cloud Function

`gcloud functions deploy body_extraction --runtime python37 --trigger-topic body_extraction --memory 2048 --set-env-vars DB_NAME=,DB_USER=,DB_PASSWORD=,DB_HOST=,DB_PORT= --timeout 540 --max-instances 1`

Dragnet package needs lxml, cython and numpy installed for us to build it properly. So what we do is build it locally and store it in the `libs` folder and then push the function code along with the libraries to the cloud function.

`pip install -t libs dragnet && rm -rf libs/*.dist-info`

The parameter -t libs might help you. You can use it to install everything on a libs folder and then you can just move the content to the local directory. As a single command it would look like this:

I added the rm -rf libs/*.dist-info portion in order to not pollute the source folder with tons of library version and distribution information that are useless to the function. Those are used by pip when freezing and planning updates.

### See logs

`gcloud functions logs read --limit 50`


## Testing

### Testing Locally

* call function `test_body_extraction`
* add above params to the params
* comment out `insert_stories`
* run `python main.py -m test_index()`

### Trigger function

`gcloud pubsub topics publish extract_body_test --message random`


[Some Links](https://stackoverflow.com/questions/56546098/deploy-a-python-cloud-function-with-all-package-dependencies)
