# dynamodb-replicator

[dynamodb-replicator](https://github.com/mapbox/dynamodb-replicator) offers three different mechanisms to provide redundancy and recoverability on DynamoDB tables using a primary-replica model:

- A **replicator** function that processes events in a Kinesis stream of DynamoDB changes made to the primary table and writes them to a replica DynamoDB table. dynamodb-replicator is compatible with the upcoming DynamoDB streams ([in preview](http://dynamodb-preview.s3-website-us-west-2.amazonaws.com/docs/streams-dg/About.html)) as well as [dyno](https://github.com/mapbox/dyno) with Kinesis ([available already](https://github.com/mapbox/dyno#multi--kinesisconfig)). The function is designed to be run as an [AWS Lambda function](http://aws.amazon.com/documentation/lambda/), optionally with deployment assistance from [streambot](https://github.com/mapbox/streambot).
- A **diff-tables** script to that scans the primary table, and checks that each individual record in the replica table is up-to-date. The goal is to double-check that the replicator is performing as is should, and the two tables are completely consistent.
- A **backup-table** script that scans a single table, and writes the data to files on S3.

### Design

Replication involves many moving parts, of which dynamodb-replicator is only one. Please read [DESIGN.md](https://github.com/mapbox/dynamodb-replicator/blob/master/DESIGN.md) for an in-depth explanation.

### replicator usage

The replicator function is designed to be run as an [AWS Lambda function](http://aws.amazon.com/documentation/lambda/) reading from an [AWS Kinesis stream](http://aws.amazon.com/documentation/kinesis/). The replicator function anticipates that individual records in the stream contain the keys for items that have been written, changed or deleted from the primary DynamoDB table. It is built for compatibilty with the upcoming release of [DynamoDB streams](http://dynamodb-preview.s3-website-us-west-2.amazonaws.com/docs/streams-dg/About.html), and presently works with write performed by [dyno with Kinesis configuration](https://github.com/mapbox/dyno#multi--kinesisconfig).

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
