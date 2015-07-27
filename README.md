# dynamodb-replicator

[dynamodb-replicator](https://github.com/mapbox/dynamodb-replicator) offers several different mechanisms to manage redundancy and recoverability on [DynamoDB](http://aws.amazon.com/documentation/dynamodb) tables.

- A **replicator** function that processes events from a DynamoDB stream, replaying changes made to the primary table and onto a replica table. The function is designed to be run as an [AWS Lambda function](http://aws.amazon.com/documentation/lambda/), optionally with deployment assistance from [streambot](https://github.com/mapbox/streambot).
- An **incremental backup** function that processes events from a DynamoDB stream, replaying them as writes to individual objects on S3. The function is designed to be run as an [AWS Lambda function](http://aws.amazon.com/documentation/lambda/), optionally with deployment assistance from [streambot](https://github.com/mapbox/streambot).
- A **consistency check** script that scans the primary table and checks that each individual record in the replica table is up-to-date. The goal is to double-check that the replicator is performing as is should, and the two tables are completely consistent.
- A **table dump** script that scans a single table, and writes the data to a file on S3, providing a snapshot of the table's state.
- A **snapshot** script that scans an S3 folder where incremental backups have been made, and writes the aggregate to a file on S3, providing a snapshot of the backup's state.

## Design

Managing table redundancy and backups involves many moving parts. Please read [DESIGN.md](https://github.com/mapbox/dynamodb-replicator/blob/master/DESIGN.md) for an in-depth explanation.

## Utility scripts

[dynamodb-replicator](https://github.com/mapbox/dynamodb-replicator) provides several CLI tools to help manage your DynamoDB table.

### diff-record

Given two tables and an item's key, this script looks up the record in both tables and checks for consistency.

```
$ npm install -g dynamodb-replicator
$ diff-record --help

Usage: diff-record <primary region/table> <replica region/table> <key>

# Check for discrepancies between an item in two tables
$ diff-record us-east-1/primary eu-west-1/replica '{"id":"abc"}'
```

### diff-tables

Given two tables and a set of options, performs a complete consistency check on the two, optionally repairing records in the replica table that differ from the primary.

```
$ npm install -g dynamodb-replicator
$ diff-tables --help

Usage: diff-tables primary-region/primary-table replica-region/replica-table

Options:
  --repair     perform actions to fix discrepancies in the replica table
  --segment    segment identifier (0-based)
  --segments   total number of segments
  --backfill   only scan primary table and write to replica

# Log information about discrepancies between the two tables
$ diff-tables us-east-1/primary eu-west-2/replica

# Repair the replica to match the primary
$ diff-tables us-east-1/primary eu-west-2/replica --repair

# Only backfill the replica. Useful for starting a new replica
$ diff-tables us-east-1/primary eu-west-2/new-replica --backfill --repair

# Perform one segment of a parallel scan
$ diff-tables us-east-1/primar eu-west-2/replica --repair --segment 0 --segments 10
```

### backup-table

Scans a table and dumps the entire set of records as a line-delimited JSON file on S3.

```
$ npm install -g dynamodb-replicator
$ backup-table --help

Usage: backup-table region/table s3url

Options:
  --jobid      assign a jobid to this backup
  --segment    segment identifier (0-based)
  --segments   total number of segments
  --metric     cloudwatch metric namespace. Will provide dimension TableName = the name of the backed-up table.

# Writes a backup file to s3://my-bucket/some-prefix/<random string>/0
$ backup-table us-east-1/primary s3://my-bucket/some-prefix

# Specifying a jobid guarantees the S3 location
# Writes a backup file to s3://my-bucket/some-prefix/my-job-id/0
$ backup-table us-east-1/primary s3://my-bucket/some-prefix --jobid my-job-id

# Perform one segment of a parallel backup
# Writes a backup file to s3://my-bucket/some-prefix/my-job-id/4
$ backup-table us-east-1/primary s3://my-bucket/some-prefix --jobid my-job-id --segment 4 --segments 10
```

### incremental-backfill

Scans a table and dumps each individual record as an object to a folder on S3.

```
$ npm install -g dynamodb-replicator
$ incremental-backfill --help

Usage: incremental-backfill region/table s3url

# Write each item in the table to S3. `s3url` should provide any desired bucket/prefix.
# The name of the table will be appended to the s3 prefix that you provide.
$ incremental-backfill us-east-1/primary s3://dynamodb-backups/incremental
```

### incremental-snapshot

Reads each item in an S3 folder representing an incremental table backup, and writes an aggregate line-delimited JSON file to S3.

```
$ npm install -g dynamodb-replicator
$ incremental-snapshot --help

Usage: incremental-snapshot <source> <dest>

Options:
  --metric     cloudwatch metric region/namespace/tablename. Will provide dimension TableName = the tablename.

# Aggregate all the items in an S3 folder into a single snapshot file
$ incremental-snapshot s3://dynamodb-backups/incremental/primary s3://dynamodb-backups/snapshots/primary
```

### incremental-diff-record

Checks for consistency between a DynamoDB record and its backed-up version on S3.

```
$ npm install -g dynamodb-replicator
$ bin/incremental-diff-record.js --help

Usage: incremental-diff-record <tableinfo> <s3url> <recordkey>
 - tableinfo: the table where the record lives, specified as `region/tablename`
 - s3url: s3 folder where the incremental backups live
 - recordkey: the key for the record specified as a JSON object

# Check that a record is up-to-date in the incremental backup
$ incremental-diff-record us-east-1/primary s3://dynamodb-backups/incremental/primary '{"id":"abc"}'
```

### incremental-backup-record

Copies a DynamoDB record's present state to an incremental backup folder on S3.

```
$ npm install -g dynamodb-replicator
$ bin/incremental-backup-record.js --help

Usage: incremental-backup-record <tableinfo> <s3url> <recordkey>
 - tableinfo: the table to backup from, specified as `region/tablename`
 - s3url: s3 folder into which the record should be backed up to
 - recordkey: the key for the record specified as a JSON object

# Backup a single record to S3
$ incremental-backup-record us-east-1/primary s3://dynamodb-backups/incremental/primary '{"id":"abc"}'
```
