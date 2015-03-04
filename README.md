# dynamodb-replicator

[dynamodb-replicator](https://github.com/mapbox/dynamodb-replicator) consumes a Kinesis stream of DynamoDB changes (keys-only) and writes them to a replica DynamoDB table. dynamodb-replicator is compatible with the upcoming DynamoDB streams ([in preview](http://dynamodb-preview.s3-website-us-west-2.amazonaws.com/docs/streams-dg/About.html)) as well as [dyno](https://github.com/mapbox/dyno) with Kinesis ([available already](https://github.com/mapbox/dyno#multi--kinesisconfig)).

[dynamodb-replicator](https://github.com/mapbox/dynamodb-replicator) also provides a `diff-tables` script to compare two tables and bring them in sync with one another.

### Features

- Primary-Replica replication between DynamoDB tables in different regions
- Replication streaming based on Kinesis
- Stream consists of object ids only (_KEYS_ONLY_), no changes or full items
- Compatible with [upcoming DynamoDB Streams](http://dynamodb-preview.s3-website-us-west-2.amazonaws.com/docs/streams-dg/About.html) and current [dyno with Kinesis](https://github.com/mapbox/dyno#multi--kinesisconfig)
- Ability to replay old stream events for bootstrapping a new replica, disaster recovery and ensuring consistency

### Design

Replication involves many moving parts, of which dynamodb-replicator is only one. Please read [DESIGN.md](https://github.com/mapbox/dynamodb-replicator/blob/master/DESIGN.md) for an in-depth explaination.

### Replicator usage

dynamodb-replicator is designed to be used along with the [Node Kinesis Client Library](https://github.com/evansolomon/nodejs-kinesis-client-library) in your own project:

With a `consumer.js`:

```
var kcl = require('kinesis-client-library');
var replicator = require('dynamodb-replicator');

var config = {
    primary: {
        region: process.env.PrimaryRegion,
        table: process.env.PrimaryTable
    },
    replica: {
        region: process.env.ReplicaRegion,
        table: process.env.ReplicaTable
    }
};

kcl.AbstractConsumer.extend(replicator(config));
```

Then start from the node kinesis client library [cli](https://github.com/evansolomon/nodejs-kinesis-client-library#cli)

```
launch-kinesis-cluster \
  --consumer ./consumer.js \
  --table kinesis-cluster-replica \
  --stream kinesis-stream-name
```

### diff-tables usage

```
$ npm install -g dynamodb-replicator
$ diff-tables --help

Usage: diff-tables primary-region/primary-table replica-region/replica-table

Options:
  --repair     perform actions to fix discrepancies in the replica table
  --segment    segment identifier (0-based)
  --segments   total number of segments
  --backfill   only scan primary table and write to replica

# log information about discrepancies between the two tables
$ diff-tables us-east-1/primary eu-west-2/replica

# repair the replica to match the primary
$ diff-tables us-east-1/primary eu-west-2/replica --repair

# Only backfill the replica. Useful for starting a new replica
$ diff-tables us-east-1/primary eu-west-2/new-replica --backfill --repair

# Perform one segment of a parallel scan
$ diff-tables us-east-1/primar eu-west-2/replica --repair --segment 0 --segments 10
```

To repa
