# dynamodb-replicator

[dynamodb-replicator](https://github.com/mapbox/dynamodb-replicator) offers three different mechanisms to provide redundancy and recoverability on DynamoDB tables using a primary-replica model:

- A **replicator** function that processes records from a Kinesis stream of DynamoDB changes made to the primary table and writes them to a replica DynamoDB table. dynamodb-replicator is compatible with the upcoming DynamoDB streams ([in preview](http://dynamodb-preview.s3-website-us-west-2.amazonaws.com/docs/streams-dg/About.html)) as well as [dyno](https://github.com/mapbox/dyno) with Kinesis ([available already](https://github.com/mapbox/dyno#multi--kinesisconfig)). The function is designed to be used along with the [Node Kinesis Client Library](https://github.com/evansolomon/nodejs-kinesis-client-library) in your own project.
- A **diff-tables** script to that scans the primary table, and checks that each individual record in the replica table is up-to-date
- A **backup-table** script that scans a single table, and writes the data to files on S3

### Design

Replication involves many moving parts, of which dynamodb-replicator is only one. Please read [DESIGN.md](https://github.com/mapbox/dynamodb-replicator/blob/master/DESIGN.md) for an in-depth explaination.

### replicator usage

The replicator function is designed to be used along with the [Node Kinesis Client Library](https://github.com/evansolomon/nodejs-kinesis-client-library) in your own project:

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

### backup-table usage

```
$ npm install -g dynamodb-replicator
$ backup-table --help

Usage: backup-table region/table s3url

Options:
  --jobid      assign a jobid to this backup
  --segment    segment identifier (0-based)
  --segments   total number of segments

# Writes a backup file to s3://my-bucket/some-prefix/<random string>/0
$ backup-table us-east-1/primary s3://my-bucket/some-prefix

# Specifying a jobid guarantees the S3 location
# Writes a backup file to s3://my-bucket/some-prefix/my-job-id/0
$ backup-table us-east-1/primary s3://my-bucket/some-prefix --jobid my-job-id

# Perform one segment of a parallel backup
# Writes a backup file to s3://my-bucket/some-prefix/my-job-id/4
$ backup-table us-east-1/primary s3://my-bucket/some-prefix --jobid my-job-id --segment 4 --segments 10
```
