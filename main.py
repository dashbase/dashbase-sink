from google.cloud import storage
from confluent_kafka import Producer
from dashsink_utils.MessagePackBuilder import MessagePackDocBuilder
import ujson, zulu

kafka_server = '130.211.225.66'
topic = 'test'

# Is the nested map supported?
schema = {
    "logName": "sorted",
    "resource": "text",
    "timestamp": "int",
    "receiveTimestamp": "int",
    "severity": "keyword",
    "insertId": "sorted",
    "httpRequest": "text",
    "labels": "text",
    "metadata": "text",
    "operation": "text",
    "trace": "sorted",
    "spanId": "sorted",
    "traceSampled": "sorted",
    "sourceLocation": "text",
    "protoPayload": "text",
    "textPayload": "text",
    "jsonPayload": "text",
}


# TODO parse gcloud log entry
def dash_sink(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")
    data = get_blob_data(bucket_name='dashbase-stackdriver-logging', source_blob_name=file['name']).decode().strip()
    logs = data.split('\n')
    producer = get_producer(kafka_server)
    builder = MessagePackDocBuilder()
    for log in logs:
        builder.reset()
        logEntry = ujson.loads(log)
        for key in logEntry.keys():
            dashbase_type = schema[key]
            value = logEntry[key]
            if key == 'timestamp':
                value = zulu.parse(value).timestamp()
                builder.set_timestamp(value)
                continue
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
        produce_data(producer, topic, builder.build(), key='key')
    producer.flush()


def get_blob_data(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    return blob.download_as_string()


def get_producer(server, port='9092'):
    conf = {'bootstrap.servers': '{}:{}'.format(kafka_server, port), 'client.id': 'test', 'default.topic.config': {'acks': 'all'}}
    producer = Producer(conf)
    return producer


def produce_data(producer, topic, data, key=None):
    producer.produce(topic, key=key, value=data)


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
    print(k.hex())

    # global kafka_server
    # kafka_server = 'localhost'
    # dash_sink({'name': 'web/2018/12/30/00:00:00_00:59:59_S0.json'}, None)


if __name__ == '__main__':
    main()
