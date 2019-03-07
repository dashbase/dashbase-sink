test_template='''{
  "index_patterns": [
    "%s"
  ],
  "mappings": {
    "_doc": {
      "_source": {
        "enabled": true
      },
      "properties": {
        "template-test": {
          "type": "text"
        }
      }
    }
  }
}'''