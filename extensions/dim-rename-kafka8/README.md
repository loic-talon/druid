### Kafka renames
This extension enables reading from a kafka feed which has name/key pairs to allow renaming of dimension values. An example use case would be to rename an ID to a human readable format.

Currently the historical node caches the key/value pairs from the kafka feed in an ephemeral memory mapped DB via MapDB.

Current limitations:
* All rename feeds must be known at server startup and are not dynamically configurable
* The query issuer must know the name of the kafka topic

### Configuration
The following options are used to define the behavior:

|Property|Description|Default|
|--------|-----------|-------|
|`druid.query.rename.kafka.properties`|A json map of kafka consumer properties. See below for special properties.|See below|

The following are the handling for kafka consumer properties in `druid.query.rename.kafka.properties`

|Property|Description|Default|
|--------|-----------|-------|
|`zookeeper.connect`|Zookeeper connection string|`localhost:2181/kafka`|
|`group.id`|Group ID, auto-assigned for publish-subscribe model and cannot be overridden|`UUID.randomUUID().toString()`|
|`auto.offset.reset`|Setting to get the entire kafka rename stream. Cannot be overridden|`smallest`|

### Hooking up namespaces
To hook up a kafka topic to a namespace, a `KafkaExtractionNamespace` used in the namespace update posted to zookeeper.

### Querying
All kafka topics are simply namespaced extraction functions.
```
    "dimExtractionFn" : {
      "type":"namespace","namespace":"testTopicNamespace"
    }
```

### Testing the rename functionality
To test this setup, you can send key/value pairs to a kafka stream via the following producer console
`./bin/kafka-console-producer.sh --property parse.key=true --property key.separator="->" --broker-list localhost:9092 --topic testTopic`
Renames can then be published as `OLD_VAL->NEW_VAL` followed by newline (enter)