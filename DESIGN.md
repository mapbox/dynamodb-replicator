# Design

## Replication

This replication system is built such that there is a **primary** table and a **replica** table. All writes are performed against the primary table, and changes made to the primary table are pushed to the replica table. Reads can be performed against either the primary or replica table.

[Dyno](https://github.com/mapbox/dyno), the client that we use for interactions with DynamoDB can be [configured to read from one table and write to another](https://github.com/mapbox/dyno#multi--kinesisconfig). Also, by providing dyno with information about an existing Kinesis stream, it can write a record to that stream each time it makes a write to the primary DynamoDB table. This Kinesis stream plays the role of a surrogate DynamoDB stream. The primary table has a Kinesis stream associated with it, while the replica table does not.

The actual replication is performed via an [AWS Lambda function](https://github.com/mapbox/dynamodb-replicator/blob/master/index.js) which reads from the primary table's Kinesis stream, and duplicates those changes in the replica table.

```
 us-east-1                         eu-west-1
-----------                       -----------
|   api   |                       |   api   |
-----------                       -----------
  ↑     |                           |     ↑
  |     |                           |     |
  |     ↓                           |     |
  |   writes ←----------------------+     |
  |   |    |                              |
reads |    |                            reads
  |   |    +~~~~ kinesis stream ~~~~+     |
  |   |                             |     |
  |   |                           writes  |
  |   |                             |     |
  |   ↓                             ↓     |
-----------                       -----------
|         |                       |         |
| primary |                       | replica |
|  table  |                       |  table  |
|         |                       |         |
-----------                       -----------
```

## Repair

The [diff-tables script provided by dynamodb-replicator](https://github.com/mapbox/dynamodb-replicator/blob/master/bin/diff-tables.js) slowly scans the primary table, and record-by-record checks that the replica table's data is up-to-date. If it encounters any discrepancies, the data in the replica table is updated to match the primary table. This script can also be used to backfill a brand new replica table.

```
        primary                                     replica
------------------------                    ------------------------
| hash | range | value |                    | hash | range | value |
------------------------ --+                ------------------------
|  10  |   1   |   1   |   |  get set   --→ |  10  |   1   |   1   | ✔
------------------------   | of primary     ------------------------
|  10  |   2   |   7   |   |  records   --→ |  10  |   2   |   7   | ✔
------------------------   |                ------------------------
|  11  |   1   |   3   |   | check each --→ |  11  |   1   |   3   | ✔
------------------------   | individual     ------------------------
|  12  |   1   |   4   |   | in replica --→ |  12  |   1   |   8   | ✘ Repair this!
------------------------ --+                ------------------------

                            ...repeat...

------------------------ --+                ------------------------
|  13  |   1   |   3   |   |  get set   --→ |  13  |   1   |   3   | ✔
------------------------   | of primary     ------------------------
|  13  |   2   |   5   |   |  records   --→ |  13  |   2   |   7   | ✘ Repair this!
------------------------   |                ------------------------
|  14  |   1   |   4   |   | check each --→ |  14  |   1   |   4   | ✔
------------------------   | individual     ------------------------
|  15  |   1   |   6   |   | in replica --→ |  15  |   1   |   6   | ✔
------------------------ --+                ------------------------
```

This gives us a system where changes to the primary table are rapidly implemented in the replica tables via Kinesis + Lambda **replication**. The **replica repair** system gives us additional certainty that replication is doing its job, and provides a system by which we can recover a database in one region by reading the data out of a table in another region.

## Backup and Restore

The backup-table script [reads each record from a table and writes it to a file on S3](https://github.com/mapbox/dynamodb-replicator/blob/master/bin/backup-table.js). Running this script on a regular basis helps build resiliency against data corruption: if corrupt data were located, a trail of past states exists on S3 that can be recovered from. 

[Dyno has a CLI](https://github.com/mapbox/dyno#usage) that includes an `import` command that can be used to read the backup data back into a table. This can be used to recover an entire database from an S3 backups. Running a restore looks something like:

```sh
$ npm install -g s3print dyno
$ s3print s3://my-bucket/my-database-backup | dyno put us-east-1/my-new-database
