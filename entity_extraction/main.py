import base64

from google.cloud import language_v1
from google.cloud.language_v1 import enums
from google.protobuf.json_format import MessageToDict


def sample_analyze_entities(text_content):
    """
    Analyzing Entities in a String

    Args:
      text_content The text content to analyze
    """

    client = language_v1.LanguageServiceClient()

    type_ = enums.Document.Type.PLAIN_TEXT

    language = "en"
    document = {"content": text_content, "type": type_, "language": language}

    encoding_type = enums.EncodingType.UTF8

    response = client.analyze_entities(document, encoding_type=encoding_type)

    dict_obj = MessageToDict(response)
    return dict_obj


def extract(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """

    print("""This Function was triggered by messageId {} published at {}

    """.format(context.event_id, context.timestamp))

    if 'data' in event:
        text = base64.b64decode(event['data']).decode('utf-8')
    else:
        print("no text in the message")
        return        

    print('Analyzing {}'.format(text[:100]))

    obj = sample_analyze_entities(text)
    print(obj)
    return obj
