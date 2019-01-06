from google.cloud import storage
from confluent_kafka import Producer
from enum import Enum, unique
import msgpack
import struct
import ujson

kafka_server = '130.211.225.66'
topic = 'test'

# Is the nested map supported?
schema = {
    "logName": "sorted",
    "resource": {
        "sorted",
    },
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


@unique
class ExtensionType(Enum):
    Sorted = 0
    Keyword = 1
    TextOffset = 2
    SortedOffset = 3
    LatLon = 4


# TODO Exception Catch
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
        value = self.value_to_bytes(value)
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = value
        return self

    def put_text_offset(self, key, start, end):
        value = self.offset_to_value(ExtensionType.TextOffset, start, end)
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = value
        return self

    def put_sorted(self, key, value):
        value = msgpack.ExtType(ExtensionType.Sorted.value, self.value_to_bytes(value))
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = value
        return self

    def put_sorted_offset(self, key, start, end):
        value = self.offset_to_value(ExtensionType.SortedOffset, start, end)
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = value
        return self

    def put_lat_lon(self, key, lat, lon):
        value = self.lat_lon_to_value(ExtensionType.LatLon, lat, lon)
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = value
        return self

    def put_keyword(self, key, value):
        value = msgpack.ExtType(ExtensionType.Keyword.value, self.value_to_bytes(value))
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = value
        return self

    def put_int(self, key, value):
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = value
        return self

    def put_long(self, key, value):
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = value
        return self

    def put_float(self, key, value):
        # It is precise, same as double in JAVA
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = float(value)
        return self

    def put_double(self, key, value):
        # There is no double type in python, use float() here
        if key in self.__dic.keys():
            value = self.add_multi_value(self.__dic[key], value)
        self.__dic[key] = float(value)
        return self

    def build(self):
        self.packer.reset()
        self.packer.pack(self.__timestamp)
        self.packer.pack(self.__raw)
        for key in self.__dic.keys():
            self.packer.pack(key)
            value = self.__dic[key]
            if isinstance(value, list):
                self.packer.pack_array_header(len(value))
                for v in value:
                    self.packer.pack(v)
            else:
                self.packer.pack(self.__dic[key])
        return self.packer.bytes()

    @staticmethod
    def offset_to_value(ext: ExtensionType, start, end):
        bytes_array = struct.pack('>ii', start, end)
        return msgpack.ExtType(ext.value, bytes_array)

    @staticmethod
    def lat_lon_to_value(ext: ExtensionType, lat, lon):
        bytes_array = struct.pack('>dd', lat, lon)
        return msgpack.ExtType(ext.value, bytes_array)

    @staticmethod
    def value_to_bytes(value):
        if isinstance(value, bytes):
            return value
        return str(value).encode()

    @staticmethod
    def add_multi_value(pre_value, value):
        if isinstance(pre_value, list):
            pre_value.append(value)
            return pre_value
        value_list = [pre_value, value]
        return value_list


# TODO parse gcloud log entry
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
    builder = MessagePackDocBuilder()
    for log in logs:
        logEntry = ujson.loads(log)
        for key in logEntry.keys():
            dashbase_type = schema[key]
            if dashbase_type is 'int':
                builder.put_int(key, logEntry[key])
            elif dashbase_type is 'text':
                builder.put_text(key, logEntry[key])
            elif dashbase_type is 'double':
                builder.put_double(key, logEntry[key])
            elif dashbase_type is 'sorted':
                builder.put_sorted(key, logEntry[key])
            elif dashbase_type is 'keyword':
                builder.put_keyword(key, logEntry[key])
        produce_data(producer, topic, builder.build(), key='key')
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
