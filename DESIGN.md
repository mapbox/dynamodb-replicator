# Design

### Flow of events and data

- All writes (_put_, _update_, _delete_) from all regions go to the DynamoDB primary table
- When an item's write to the DynamoDB primary table is confirmed, the item key is written to the Kinesis stream in the primary region
- An instance of [dynamodb-replicator](https://github.com/mapbox/dynamodb-replicator) consumes the Kinesis stream in the primary region
    - For each item key on the Kinesis stream, [dynamodb-replicator](https://github.com/mapbox/dynamodb-replicator) reads the item as DynamoDB consistent read from the Primary table and writes the item to the Replica table

```
primary region

+----------------+ +----------+
|DynamoDB Primary| |Kinesis   |
+-^--------------+ ++---------+
  |                 |
  |read             |put
  |write            |
  |                 |
  |                 |
+-+-------------+   |
|dyno           +---^
+---------------+
```

```
primary region                       replica region     
                                                        
+----------------+ +----------+      +----------------+ 
|DynamoDB Primary| |Kinesis   |      |DynamoDB Replica| 
+-^------^-------+ +^-----^---+      +^-----------^---+ 
  |      |          |     |           |           |     
  |read  |write     |put  |consume    |read       |write
  |      |          |     |           |           |     
  |      |          |     |          ++-----+     |     
  |      ^----------+----------------+dyno  |     |     
  |                       |          +------+     |     
  |                       |                       |     
  |       +---------------+---+                   |     
  ^-------+dynamodb replicator+-------------------+     
          +-------------------+                         
```

### Expected Kinesis record format

```
{
    "awsRegion": "us-east-1",
    "dynamodb": {
        "Keys": {
            "ForumName": {"S": "DynamoDB"},
            "Subject": {"S": "DynamoDB Thread 3"}
        },
        "SequenceNumber": "300000000000000499659",
        "SizeBytes": 41,
        "StreamViewType": "KEYS_ONLY"
    },
    "eventID": "e2fd9c34eff2d779b297b26f5fef4206",
    "eventName": "INSERT",
    "eventSource": "aws:dynamodb",
    "eventVersion": "1.0"
},
```

The record format is compatible with the format of the [DynamoDB Streams Preview](http://dynamodb-preview.s3-website-us-west-2.amazonaws.com/docs/streams-dg/About.html).