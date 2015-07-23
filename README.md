# dynamodb-replicator

[dynamodb-replicator](https://github.com/mapbox/dynamodb-replicator) offers several different mechanisms to provide redundancy and recoverability on DynamoDB tables using a primary-replica model:

- A **replicator** function that processes events in a DynamoDB stream representing changes made to the primary table and writes them to a replica DynamoDB table. The function is designed to be run as an [AWS Lambda function](http://aws.amazon.com/documentation/lambda/), optionally with deployment assistance from [streambot](https://github.com/mapbox/streambot).
- An **incremental backup** function that processes events in a DynamoDB stream representing changes made to the primary table and writes them as individual objects to a location on S3. The function is designed to be run as an [AWS Lambda function](http://aws.amazon.com/documentation/lambda/), optionally with deployment assistance from [streambot](https://github.com/mapbox/streambot).
- A **diff-tables** script to that scans the primary table, and checks that each individual record in the replica table is up-to-date. The goal is to double-check that the replicator is performing as is should, and the two tables are completely consistent.
- A **backup-table** script that scans a single table, and writes the data to files on S3.

### Design

Replication involves many moving parts, of which dynamodb-replicator is only one. Please read [DESIGN.md](https://github.com/mapbox/dynamodb-replicator/blob/master/DESIGN.md) for an in-depth explanation.

### replicator usage

The replicator function is designed to be run as an [AWS Lambda function](http://aws.amazon.com/documentation/lambda/) reading from a table's [DynamoDB stream](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html). [Streambot](https://github.com/mapbox/streambot) is a helpful library to assist in deploying Lambda functions and managing runtime configuration that the replication function will need.

### incremental backups

dynamodb-replicator provides functions that can help maintain an S3 folder where each object represents the current state of a record from a DyanmoDB table. In a [versioned bucket](), this can provide a history of changes made to the table.

Dynamodb-replicator provides three useful routines for maintaining an S3 clone, or incremental backup, of your table:
- An incremental backup function, designed to run as a Lambda function that consumes records from your table's DynamoDB stream and writes each item to S3
- An `incremental-backfill` script, run once to fill the existing table state into the S3 folder
- An `incremental-snapshot` script, which you can run periodically to produce a snapshot of your database state at some point in time. This snapshot is compatible with the restore function mentioned above.

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
