cloudtrail_template = '''{
  "index_patterns": [
    "%s"
  ],
  "mappings": {
    "_doc": {
      "_source": {
        "enabled": true
      },
      "properties": {
        "eventVersion": {
          "type": "keyword"
        },
        "userIdentity": {
          "properties": {
            "type": {
              "type": "keyword"
            },
            "principalId": {
              "type": "keyword"
            },
            "arn": {
              "type": "keyword"
            },
            "accountId": {
              "type": "keyword"
            },
            "accessKeyId": {
              "type": "keyword"
            },
            "userName": {
              "type": "keyword"
            }
          }
        },
        "eventSource": {
          "type": "keyword"
        },
        "eventName": {
          "type": "keyword"
        },
        "awsRegion": {
          "type": "keyword"
        },
        "sourceIPAddress": {
          "type": "keyword"
        },
        "userAgent": {
          "type": "keyword"
        },
        "requestParameters": {
          "type": "text"
        },
        "responseElements": {
          "type": "text"
        }
      }
    }
  }
}'''
