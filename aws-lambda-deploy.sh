#!/bin/bash
source env/bin/activate && \
pip install -r requirements.txt && \
cd env/lib/python3.7/site-packages/ && \
zip -r9 ../../../../cloudwatchToDashbase.zip * && \
cd ../../../../ && \
zip -r9 ./cloudwatchToDashbase.zip dashsink_utils/ && \
zip -g ./cloudwatchToDashbase.zip ./cloudwatchToDashbase.py && \
aws lambda update-function-code --function-name cloudtrailToDashbase --zip-file fileb://cloudwatchToDashbase.zip --region us-west-2
