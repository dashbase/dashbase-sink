from google.cloud import storage
from confluent_kafka import Producer
from enum import Enum, unique
import msgpack

kafka_server = '130.211.225.66'
topic = 'test'


@unique
class ExtensionType(Enum):
    Sorted = 0
    Keyword = 1
    TextOffset = 2
    SortedOffset = 3
    LatLon = 4


# TODO Exception Catch
# TODO More ExtensionType
# TODO INT LONG FLOAT DOUBLE type
# TODO Multi Value
# TODO add test
class MessagePackDocBuilder:

    def __init__(self):
        self.__timestamp = ''
        self.__raw = ''
        self.__dic = dict()
        self.packer = msgpack.Packer(use_bin_type=True, autoreset=False)

    def set_timestamp(self, timestamp):
        timestamp = int(timestamp)
        self.__timestamp = timestamp
        return self

    def get_timestamp(self):
        return self.__timestamp

    def set_raw(self, raw):
        if isinstance(raw, str):
            raw = raw.encode()
        self.__raw = raw
        return self

    def get_raw(self):
        return self.__raw

    def reset(self):
        self.__timestamp = ''
        self.__raw = ''
        self.__dic = dict()
        self.packer.reset()
        return self

    def put_text(self, key, value):
        if isinstance(value, str):
            value = value.encode()
        self.__dic[key] = value
        return self

    def put_sorted(self, key, value):
        if isinstance(value, str):
            value = value.encode()
        self.__dic[key] = msgpack.ExtType(ExtensionType.Sorted.value, value)
        return self

    def put_keyword(self, key, value):
        if isinstance(value, str):
            value = value.encode()
        self.__dic[key] = msgpack.ExtType(ExtensionType.Keyword.value, value)
        return self

    def put_int(self, key, value):
        self.__dic[key] = value
        return self

    def put_long(self, key, value):
        self.__dic[key] = value
        return self

    def put_float(self, key, value):
        # It is precise, same as double in JAVA
        self.__dic[key] = float(value)
        return self

    def put_double(self, key, value):
        # There is no double type in python, use float() here
        self.__dic[key] = float(value)
        return self

    def build(self):
        self.packer.reset()
        self.packer.pack(self.__timestamp)
        self.packer.pack(self.__raw)
        for key in self.__dic.keys():
            self.packer.pack(key)
            self.packer.pack(self.__dic[key])
        return self.packer.bytes()


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
        msgpack_log = msgpack.packb(log, use_bin_type=True)
        produce_data(producer, topic, msgpack_log, key='key')
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
    builder = MessagePackDocBuilder()
    builder.set_timestamp(1234)
    builder.set_raw('This is test')
    builder.put_text('text', 'text123')
    builder.put_int('int', 111)
    builder.put_float('float', 12.34)
    k = builder.build()
    print(k.hex())

    # global kafka_server
    # kafka_server = 'localhost'
    # dash_sink({'name': 'web/2018/12/30/00:00:00_00:59:59_S0.json'}, None)


if __name__ == '__main__':
    main()
