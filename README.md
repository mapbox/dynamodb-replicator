# Dynamodb Replicator


This provides a consumer for a kinesis stream of dynamodb changes. This can be used with dynamo streams (in preview) or with a [dyno](https://github.com/mapbox/dyno) client that is configured to write to a kinesis stream.


### Expected record format

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
