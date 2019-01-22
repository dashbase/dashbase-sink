from __future__ import print_function
from confluent_kafka import Producer
import boto3
import zlib
import os
import time

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


# TODO serialize data
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
            for line in lines:
                produce_data(producer, topic, line)
            print("=>   send usage time: {}s".format(time.time() - start_time))
            print("s3 file:{} transfer to kafka successful ".format(key))

            return key
        except Exception as e:
            raise e
