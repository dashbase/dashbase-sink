#!/bin/bash
source env/bin/activate && \
pip install -r requirements.txt && \
cd env/lib/python3.7/site-packages/ && \
zip -r9 ../../../../s3ToKafka.zip * && \
cd ../../../../ && \
zip -r9 ./s3ToKafka.zip dashsink_utils/ && \
zip -g ./s3ToKafka.zip ./s3ToKafka.py && \
aws lambda update-function-code --function-name cloudtrailToDashbase --zip-file fileb://s3ToKafka.zip --region us-west-2