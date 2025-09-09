# kafka-keys-cleaner

A simple tool to clean up old keys in Kafka topics.

P.S. With a standard Apache Kafka cluster, the same goal can be achieved by configuring `delete,compact` as the cleanup policy for a topic.

## How to build

Build the project with JDK 17 or later:

```bash
mvn clean install -DskipTests
```

## Demo against a cluster on StreamNative Cloud

After building the project, save the Kafka service URL to `./KAFKA-URL.txt` and token to `./TOKEN.txt`, then save them as environment variables:

```bash
export URL=$(cat ./KAFKA-URL.txt)
export TOKEN=$(cat ./TOKEN.txt)
```

After that, you can leverage this demo project to show how to delete keys before a timestamp.

### Step 1: Create a compacted topic

```bash
java -jar target/kafka-keys-cleaner.jar $URL test-topic --token $TOKEN compact --create
```

### Step 2: produce some messages

```bash
java -jar target/kafka-keys-cleaner.jar $URL test-topic --token $TOKEN produce -f DATA.txt
```

See [DATA.txt](./DATA.txt) for the content.

You will see outputs like:

```
Sent K0 => V0 to test-topic-0@0 timestamp: 1757433987842
Sent K0 => V1 to test-topic-0@1 timestamp: 1757433990328
Sent K1 => V0 to test-topic-0@2 timestamp: 1757433990603
Sent K2 => v0 to test-topic-0@3 timestamp: 1757433990932
Sent K1 => V1 to test-topic-0@4 timestamp: 1757433991207
Sent K0 => V2 to test-topic-0@5 timestamp: 1757433991491
```

You can try consuming these messages with `my-group` as the group id:

```bash
java -jar target/kafka-keys-cleaner.jar $URL test-topic --token $TOKEN consume my-group
```

```
Received K0 => V0 from test-topic-0@0 timestamp: 1757433987842
Received K0 => V1 from test-topic-0@1 timestamp: 1757433990328
Received K1 => V0 from test-topic-0@2 timestamp: 1757433990603
Received K2 => v0 from test-topic-0@3 timestamp: 1757433990932
Received K1 => V1 from test-topic-0@4 timestamp: 1757433991207
Received K0 => V2 from test-topic-0@5 timestamp: 1757433991491
```

### Step 3: Write timestones for keys before a timestamp

Let's use `1757433990933` as the timestamp:

```bash
java -jar target/kafka-keys-cleaner.jar $URL test-topic --token $TOKEN compact --set-key-ts=1757433990933
```

You will see the following outputs:

```
Deleted key K2
```

It's expected because the messages after this timestamp are:

```
K1 => V1
K0 => V2
```

So the outdated key `K2` will be deleted as well.

### Step 4: Verify the compacted results

You can try consuming again and will see a tombstone message for key `K2` has been written:

```bash
java -jar target/kafka-keys-cleaner.jar $URL test-topic --token $TOKEN consume my-group
```

Outputs:

```
Received K2 => null from test-topic-0@6 timestamp: 1757434254734
```

Let's use Pulsar CLI to manually trigger the compaction:

```bash
# Please note that the URL is a Pulsar admin service URL
bin/pulsar-admin \
    --admin-url <pulsar-admin-service-url> \
    --auth-plugin org.apache.pulsar.client.impl.auth.AuthenticationToken \
    --auth-params token:$TOKEN \
    topics compact test-topic
```

Wait for a while and consume again:

```bash
java -jar target/kafka-keys-cleaner.jar $URL test-topic --token $TOKEN consume my-group
```

Now, you will see the messages after compaction:

```
Received K1 => V1 from test-topic-0@4 timestamp: 1757433991207
Received K0 => V2 from test-topic-0@5 timestamp: 1757433991491
```

As a result, key `K2` has been deleted.
