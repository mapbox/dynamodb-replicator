var AWS = require('aws-sdk');
var queue = require('queue-async');
var _ = require('lodash');

exports.handler = function replicate(event, context) {
    var config = {
        region: 'us-east-1',
        endpoint: 'https://preview-dynamodb.us-east-1.amazonaws.com'
    };

    var dynamo = new AWS.DynamoDB(config);

    var q = queue();

    (function putRecord(i) {
        if (i === event.Records.length) {
            return context.succeed('Processing successful');
        }

        var record = event.Records[i];
        console.log(record.EventID);
        console.log(record.EventName);
        console.log('DynamoDB Record: %j', record.Dynamodb);

        if (record.EventName === 'INSERT' || record.EventName === 'MODIFY') {
            dynamo.putItem({
                TableName: 'jakepruitt',
                Item: record.Dynamodb.NewImage
            }, nextRecord);
        } else if (record.EventName === 'REMOVE') {
            dynamo.deleteItem({
                TableName: 'jakepruitt',
                Key: record.Dynamodb.Keys
            }, nextRecord);
        }

        function nextRecord(err, data) {
            if (err) return context.fail(err);
            return putRecord(++i);
        }
    })(0);
};
