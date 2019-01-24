from __future__ import print_function
from confluent_kafka import Producer
from dashsink_utils.MessagePackBuilder import MessagePackDocBuilder
from dashsink_utils.schema.CloudTrailSchema import cloudTrailSchema
import boto3
import zlib
import os
import time
import ujson
import zulu

schema = cloudTrailSchema
s3 = boto3.client('s3')
kafka_host = os.environ.get('KAFKA_HOST', 'localhost:9092')
topic = os.environ.get('KAFKA_TOPIC', 'DASHBASE')
print('Loading function host:{} topic:{}', kafka_host, topic)


def get_producer(host):
    conf = {'bootstrap.servers': '{}'.format(host), 'client.id': 'test', 'default.topic.config': {'acks': 'all'}}
    producer = Producer(conf)
    return producer


def produce_data(producer, topic, data, key=None):
    producer.produce(topic, key=key, value=data)


def handler(event, context):
    for record in event['Records']:
        start_time = time.time()
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            body = response['Body']
            data = body.read()
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. '.format(key, bucket))
            raise e

        print("=>   download usage time: {}s".format(time.time() - start_time))
        if key.endswith(".gz"):
            try:
                data = zlib.decompress(data, 16 + zlib.MAX_WBITS)
                print("Detected gzipped content")
                print("=>   gzip decompress usage time: {}s".format(time.time() - start_time))
            except zlib.error:
                print("Content couldn't be ungzipped, assuming plain text")
        lines = data.splitlines()
        print("=>   split time: {}s".format(time.time() - start_time))
        try:
            producer = get_producer(kafka_host)
            builder = MessagePackDocBuilder()
            for line in lines:
                builder.reset()
                logEntry = ujson.loads(line)
                for key in logEntry.keys():
                    if key in schema.keys():
                        dashbase_type = schema[key]
                    else:
                        dashbase_type = 'text'
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
            print("=>   send usage time: {}s".format(time.time() - start_time))
            print("s3 file:{} transfer to kafka successful ".format(key))
            return key
        except Exception as e:
            raise e
