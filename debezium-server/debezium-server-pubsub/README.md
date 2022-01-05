# PubSub change consumer for Debezium

Instructions TBD.

# Configuration

```properties
# messages will be published to %s_%s, where first %s is the prefix, and the second one is actual table qualifier,
# e.g. db_name.schema_name.table_name
debezium.sink.pubsub.pubsub.topic.prefix=table_prefix
```
