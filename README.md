# Dashbase-sink

### Overview

This is a google cloud function script. 

### What it does

1. It will be triggered whenever there's a new file created in the bucket(set when deployed).
2. Serialize the data in the file and export data  to dashbase cluster.

### How to deploy
```
$ gcloud components update
```
```
$ gcloud components install beta
```

```
gcloud beta functions deploy dash-sink --runtime python37 --trigger-resource dashbase-stackdriver-logging --trigger-event google.storage.object.finalize --entry-point hello_gcs
```
### Documents

Google cloud storage trigger: https://cloud.google.com/functions/docs/calling/storage?authuser=1

The event object format: https://cloud.google.com/storage/docs/json_api/v1/objects

The log entry format: https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry