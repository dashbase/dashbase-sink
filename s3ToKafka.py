from __future__ import print_function
from dashsink_utils.MessagePackBuilder import MessagePackDocBuilder
from dashsink_utils.schema.CloudTrailSchema import cloudTrailSchema
from kafka import KafkaProducer
import boto3
import zlib
import os
import time
import zulu
import json

schema = cloudTrailSchema
s3 = boto3.client('s3')
kafka_host = os.environ.get('KAFKA_HOST', '35.247.63.148:9092')
topic = os.environ.get('KAFKA_TOPIC', 'aws-cloudTrail')
print('Loading function host:{} topic:{}', kafka_host, topic)


def get_producer(host):
    producer = KafkaProducer(bootstrap_servers=[host])
    return producer


def produce_data(producer, topic, data, key=None):
    producer.send(topic, data)


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

        try:
            lines = json.loads(data)['Records']
        except Exception as e:
            lines = data.splitlines()

        print("=>   split time: {}s".format(time.time() - start_time))
        try:
            producer = get_producer(kafka_host)
            builder = MessagePackDocBuilder()
            for line in lines:
                builder.reset()
                builder.set_raw(line)
                try:
                    if isinstance(line, dict):
                        logEntry = line
                    else:
                        logEntry = json.loads(line)
                except Exception as e:
                    raise e
                for key in logEntry.keys():
                    if key in schema.keys():
                        flatten(builder, key, logEntry[key], schema[key])
                    else:
                        flatten(builder, key, logEntry[key], "text")
                produce_data(producer, topic, builder.build(), key='key')
            producer.flush()
            print("=>   send usage time: {}s".format(time.time() - start_time))
            print("s3 file:{} transfer to kafka successful ".format(key))
            return key
        except Exception as e:
            raise e


def flatten(builder, prefix, value, dashbase_type):
    if isinstance(value, dict):
        for key in value.keys():
            if isinstance(dashbase_type,dict) and key in dashbase_type.keys():
                flatten(builder, "{}.{}".format(prefix, key), value[key], dashbase_type[key])
            else:
                flatten(builder, "{}.{}".format(prefix, key), value[key], "text")
    elif isinstance(value, list):
        for v in value:
            pack(builder, prefix, v, dashbase_type)
    else:
        pack(builder, prefix, value, dashbase_type)


def pack(builder, key, value, dashbase_type):
    if key == 'eventTime':
        value = zulu.parse(value).timestamp()
        builder.set_timestamp(value)
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
