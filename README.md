# dynamodb-replicator

[dynamodb-replicator](https://github.com/mapbox/dynamodb-replicator) offers several different mechanisms to manage redundancy and recoverability on [DynamoDB](http://aws.amazon.com/documentation/dynamodb) tables.

- A **replicator** function that processes events from a DynamoDB stream, replaying changes made to the primary table and onto a replica table. The function is designed to be run as an [AWS Lambda function](http://aws.amazon.com/documentation/lambda/).
- An **incremental backup** function that processes events from a DynamoDB stream, replaying them as writes to individual objects on S3. The function is designed to be run as an [AWS Lambda function](http://aws.amazon.com/documentation/lambda/).
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

### replicate-record

Given two tables and an item's key, this script insures that the replica record is synchronized with its current state in the primary table.

```
$ npm install -g dynamodb-replicator
$ replicate-record --help

Usage: replicate-record <primary tableinfo> <replica tableinfo> <recordkey>
 - primary tableinfo: the primary table to replicate from, specified as `region/tablename`
 - replica tableinfo: the replica table to replicate to, specified as `region/tablename`
 - recordkey: the key for the record specified as a JSON object

# Copy the state of a record from the primary to the replica table
$ replicate-record us-east-1/primary eu-west-1/replica '{"id":"abc"}'
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
$ incremental-diff-record --help

Usage: incremental-diff-record <tableinfo> <s3url> <recordkey>
 - tableinfo: the table where the record lives, specified as `region/tablename`
 - s3url: s3 folder where the incremental backups live
 - recordkey: the key for the record specified as a JSON object

# Check that a record is up-to-date in the incremental backup
$ incremental-diff-record us-east-1/primary s3://dynamodb-backups/incremental '{"id":"abc"}'
```

### incremental-backup-record

Copies a DynamoDB record's present state to an incremental backup folder on S3.

```
$ npm install -g dynamodb-replicator
$ incremental-backup-record --help

Usage: incremental-backup-record <tableinfo> <s3url> <recordkey>
 - tableinfo: the table to backup from, specified as `region/tablename`
 - s3url: s3 folder into which the record should be backed up to
 - recordkey: the key for the record specified as a JSON object

# Backup a single record to S3
$ incremental-backup-record us-east-1/primary s3://dynamodb-backups/incremental '{"id":"abc"}'
```

### incremental-record-history

Prints each version of a record that is available in an incremental backup folder on S3.

```
$ incremental-record-history --help

Usage: incremental-record-history <tableinfo> <s3url> <recordkey>
 - tableinfo: the table where the record lives, specified as `region/tablename`
 - s3url: s3 folder where the incremental backups live. Table name will be appended
 - recordkey: the key for the record specified as a JSON object

# Read the history of a single record
$ incremental-record-history us-east-1/my-table s3://dynamodb-backups/incremental '{"id":"abc"}'
```

### Instructions to run:
##### Streambot dependency removed since it is deprecated

The code could be deployed on a lambda for incremental backups to S3 and for replication across DynamoDB tables across regions and accounts. 

Initial setup:
```
$ git clone https://github.com/subbuvenk94/dynamodb-replicator.git
$ cd dynamodb-replicator
$ npm install
```
After the dependent packages are installed, zip the contents of the root directory. Deploy the zip on to AWS Lambda console with the handler as ```index.backup``` or ```index.replicate``` based on your required functionality.

The Lambda console lets us set the environment variables. Refer to this:
```
## Backup Functionality
# Target bucket on S3
BackupBucket=
# Prefix for the data inside the bucket (Optional)
BackupPrefix=
# Region for the bucket (Optional)
BackupRegion=

## Replication Functionality
# Key/Secret for a different AWS account write (Optional)
ReplicaAccessKeyId=
ReplicaSecretAccesseKey=
# Name of replica table
ReplicaTable=
# Replica table region (NOTE: We have to manually create this table for the same key)
ReplicaRegion=
#Endpoint for replica table (Optional)
ReplicaEndpoint=
```
Enable triggers for the tables on which these operations are made with view type ```New and old images - both the new and the old images of the item```. Note that the 'Triggers' option on DynamoDB tables are available based on region. Create the trigger for the existing Lambda function that we just created.

For tables in which data already exists, the existing data has to be first replicated before we could do the incremental replication. Refer to [diff-tables](#diff-tables) for replication backfill. Similarly, for we can perform [incremental backfill](#incremental-backfill).

The Lambda function would need permissions to perform the incremental backup or replication tasks. The IAM policy that should be created to award this permission could be set as:
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::<BACKUP_BUCKET_NAME>"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::<BACKUP_BUCKET_NAME>/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": "lambda:InvokeFunction",
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:DescribeStream",
                "dynamodb:GetRecords",
                "dynamodb:GetShardIterator",
                "dynamodb:ListStreams",
                "dynamodb:DescribeTable",
                "dynamodb:PutItem",
                "dynamodb:BatchWriteItem",
                "dynamodb:Scan"
            ],
            "Resource": "*"
        }
    ]
}
```
NOTE: Edit the BACKUP_BUCKET_NAME to the name of the backup bucket.

After this setup, any changes to the tables should trigger the Lambda which would perform the incremental backup or the table repication. Your changes would be visible in the respective table or bucket.
