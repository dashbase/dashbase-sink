gcloud_template='''{
  "index_patterns": [
    "%s"
  ],
  "mappings": {
    "_doc": {
      "_source": {
        "enabled": true
      },
      "properties": {
        "logName": {
          "type": "keyword"
        },
        "resource": {
          "type": "keyword"
        },
        "timestamp": {
          "type": "date"
        },
        "receiveTimestamp": {
          "type": "date"
        },
        "severity": {
          "type": "keyword"
        },
        "insertId": {
          "type": "keyword"
        },
        "httpRequest": {
          "type": "text"
        },
        "labels": {
          "type": "text"
        },
        "metadata": {
          "type": "text"
        },
        "operation": {
          "type": "text"
        },
        "trace": {
          "type": "keyword"
        },
        "spanId": {
          "type": "keyword"
        },
        "traceSampled": {
          "type": "keyword"
        },
        "sourceLocation": {
          "type": "text"
        },
        "protoPayload": {
          "type": "text"
        },
        "textPayload": {
          "type": "text"
        },
        "jsonPayload": {
          "type": "text"
        }
      }
    }
  }
}'''