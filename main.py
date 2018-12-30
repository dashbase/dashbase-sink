from google.cloud import storage
from confluent_kafka import Producer
import msgpack

kafka_server = '130.211.225.66'
topic='test'

def dash_sink(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")
    data = get_blob_data(bucket_name='dashbase-stackdriver-logging', source_blob_name=file['name']).decode()
    logs = data.split('\n')
    producer = get_producer(kafka_server)
    for log in logs:
        msgpack_log=msgpack.packb(log,use_bin_type=True)
        produce_data(producer,topic,msgpack_log,key='key')
    producer.flush()
    print("Process data successfully!")


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
    global kafka_server
    kafka_server = 'localhost'
    dash_sink({'name': 'web/2018/12/30/00:00:00_00:59:59_S0.json'}, None)


if __name__ == '__main__':
    main()
