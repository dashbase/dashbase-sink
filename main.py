from google.cloud import storage
from confluent_kafka import Producer
from dashsink_utils.MessagePackBuilder import MessagePackDocBuilder
from dashsink_utils.schema.GoogleCloudLogEntrySchema import logEntrySchema
import ujson, zulu
import os

kafka_host = os.environ.get('KAFKA_HOST', '35.247.63.148:9092')
topic = os.environ.get('KAFKA_TOPIC', 'gcloud-sink')
# Is the nested map supported?
schema = logEntrySchema


# TODO parse gcloud log entry
def dash_sink(event, context):
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
    builder = MessagePackDocBuilder()
    builder.set_timestamp(1234)
    builder.set_raw('This is test')
    builder.put_text('text', 'abc')
    builder.put_text_offset('text2', 0, 4)
    builder.put_sorted("sorted", "abc")
    builder.put_sorted_offset("sorted2", 8, 12)
    builder.put_int('long', 123)
    builder.put_double('double', 12.34)
    builder.put_lat_lon('latlon', 12, 34)
    k = builder.build()
    dash_sink({'file': '1'}, 1)
    # global kafka_server
    # kafka_server = 'localhost'
    # dash_sink({'name': 'web/2018/12/30/00:00:00_00:59:59_S0.json'}, None)


if __name__ == '__main__':
    main()
