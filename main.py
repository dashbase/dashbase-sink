from google.cloud import storage
from confluent_kafka import Producer
from dashsink_utils.MessagePackBuilder import MessagePackDocBuilder
from dashsink_utils.schema.GoogleCloudLogEntrySchema import logEntrySchema
import ujson, zulu
import os
import requests

kafka_host = os.environ.get('KAFKA_HOST', '35.247.63.148:9092')
topic = os.environ.get('KAFKA_TOPIC', 'gcloud-sink')
# Is the nested map supported?
schema = logEntrySchema


# TODO parse gcloud log entry
def dash_sink_with_kafka(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")
    print(f"KafkaHost: {kafka_host}, Topic: {topic}")
    data = get_blob_data(bucket_name='dashbase-stackdriver-logging', source_blob_name=file['name']).decode().strip()
    logs = data.split('\n')
    producer = get_producer(kafka_host)
    builder = MessagePackDocBuilder()
    for log in logs:
        builder.reset()
        builder.set_raw(log)
        try:
            logEntry = ujson.loads(log)
        except Exception as e:
            raise e
        for key in logEntry.keys():
            if key in schema.keys():
                flatten(builder, key, logEntry[key], schema[key])
            else:
                flatten(builder, key, logEntry[key], "text")

        produce_data(producer, topic, builder.build(), key='key')
    producer.flush()


def dash_sink(event, context):
    file = event
    print(f"Processing file: {file['name']}.")
    data = get_blob_data(bucket_name='dashbase-stackdriver-logging', source_blob_name=file['name']).decode().strip()
    logs = data.split('\n')
    index = '{"index": { "_index": "gcloud-sink", "_type": "sink"}}\n'
    bulk_request=''
    for i in range(0,len(logs),1):
        bulk_request +=index+logs[i]+'\n'
        if (i+1) % 100==0:
            post_bulk(bulk_request.strip())
            bulk_request=''
    post_bulk(bulk_request.strip())
    bulk_request = ''
    print("All the steps finished")

def post_bulk(data):
    if data=='':
        return
    url = 'https://35.247.115.177:31514/_bulk'
    headers = {
        'content-type': 'application/json',
        'cache-control': 'no-cache'
    }
    r = requests.post(url, data=data, headers=headers, verify=False)
    print(f"Bulk result:", r.status_code)

def flatten(builder, prefix, value, dashbase_type):
    if isinstance(value, dict):
        for key in value.keys():
            if isinstance(dashbase_type, dict) and key in dashbase_type.keys():
                flatten(builder, "{}.{}".format(prefix, key), value[key], dashbase_type[key])
            else:
                flatten(builder, "{}.{}".format(prefix, key), value[key], "text")
    elif isinstance(value, list):
        for v in value:
            pack(builder, prefix, v, dashbase_type)
    else:
        pack(builder, prefix, value, dashbase_type)


def pack(builder, key, value, dashbase_type):
    if key == 'timestamp':
        value = zulu.parse(value).timestamp()
        builder.set_timestamp(value)
        return
    if key == 'receiveTimestamp':
        value = zulu.parse(value).timestamp()
    if dashbase_type is 'int':
        builder.put_int(key, value)
    elif dashbase_type is 'text':
        builder.put_text(key, value)
    elif dashbase_type is 'double':
        builder.put_double(key, value)
    elif dashbase_type is 'sorted':
        builder.put_sorted(key, value)
    elif dashbase_type is 'keyword':
        builder.put_keyword(key, value)


def get_blob_data(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    return blob.download_as_string()


def get_producer(host):
    conf = {'bootstrap.servers': '{}'.format(host), 'client.id': 'test', 'default.topic.config': {'acks': 'all'}}
    producer = Producer(conf)
    return producer


def produce_data(producer, topic, data, key=None):
    producer.produce(topic=topic, key=key, value=data)


def main():
    dash_sink(1,1)


if __name__ == '__main__':
    main()
