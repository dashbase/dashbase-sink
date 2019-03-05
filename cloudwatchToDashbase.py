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
import requests
import base64
import urllib3
from urllib3.exceptions import InsecureRequestWarning

schema = cloudTrailSchema
s3 = boto3.client('s3')
kafka_host = os.environ.get('KAFKA_HOST', '35.247.63.148:9092')
topic = os.environ.get('KAFKA_TOPIC', 'aws-cloudTrail')
es_host=os.environ.get('ES_HOST','localhost:9200')
es_index=os.environ.get('ES_INDEX','test_index')
es_subTable=os.environ.get('ES_SUBTABLE','test_table')
print('Loading function host:{} topic:{}', kafka_host, topic)
urllib3.disable_warnings(InsecureRequestWarning)


def get_producer(host):
    producer = KafkaProducer(bootstrap_servers=[host])
    return producer


def produce_data(producer, topic, data, key=None):
    producer.send(topic, data)


def s3ToKafka(event, context):
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
        except Exception as e:
            raise e


def s3ToDashbase(event, context):
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
            index = '{"index": { "_index": "%s", "_type": "%s"}}\n'%(es_index, es_subTable)
            bulk_request = ''
            for i in range(0, len(lines), 1):
                line = lines[i]
                try:
                    if isinstance(line, dict):
                        logEntry = json.dumps(line)
                    else:
                        logEntry = line.strip()
                except Exception as e:
                    raise e
                bulk_request += index + logEntry + '\n'
                if (i + 1) % 100 == 0:
                    post_bulk(bulk_request.strip())
                    bulk_request = ''
            post_bulk(bulk_request.strip())
            print("=>   send usage time: {}s".format(time.time() - start_time))
            print("All the steps finished")
        except Exception as e:
            raise e


def cloudwatchToDashbase(event, context):
    rawData = event['awslogs']['data']
    data = json.loads(zlib.decompress(base64.b64decode(rawData), 16 + zlib.MAX_WBITS))
    logs = data.pop('logEvents')
    index = '{"index": { "_index": "%s", "_type": "%s"}}\n' % (es_index, es_subTable)
    bulk_request = ''
    succ = 0
    fail = 0
    for i in range(0, len(logs), 1):
        log = logs[i]
        try:
            log['message'] = json.loads(log['message'])
        except Exception as e:
            print(e.__str__())
        if isinstance(log['message'], dict):
            message = log.pop('message')
            log.update(message)
        log.update(data)
        bulk_request += index + json.dumps(log) + '\n'
        if (i + 1) % 100 == 0:
            num1, num2 = post_bulk(bulk_request.strip())
            succ += num1
            fail += num2
            bulk_request = ''
    num1, num2 = post_bulk(bulk_request.strip())
    succ += num1
    fail += num2
    print("Successful items:{}",succ)
    print("Failed items:{}", fail)


def post_bulk(data):
    if data == '':
        return
    url = '{}/_bulk'.format(es_host)
    headers = {
        'content-type': 'application/json',
        'cache-control': 'no-cache'
    }
    r = requests.post(url, data=data, headers=headers, verify=False)
    res = r.json()
    items = res['items']
    failed = list(map(lambda x: x['index']['status'] >= 300, items)).count(True)
    print(f"Bulk result:", r.status_code)
    return len(items) - failed, failed


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
    if key == 'eventTime':
        value = zulu.parse(value).timestamp()
        builder.set_timestamp(value)
        return
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
