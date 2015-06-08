var AWS = require('aws-sdk');
var queue = require('queue-async');
var _ = require('lodash');

exports.handler = function replicate(event, context) {
    var config = {
        region: 'us-east-1',
        endpoint: 'https://preview-dynamodb.us-east-1.amazonaws.com'
    };

    var dynamo = new AWS.DynamoDB(config);

    var recordsById = actionsPerId(event.Records);

    var q = queue();

    _.forOwn(recordsById, function(records) {
        q.defer(function(nextId) {
            var serial = queue(1);

            records.forEach(function(record) {
                serial.defer(function(nextRecord) {
                    console.log(record.EventID);
                    console.log(record.EventName);
                    console.log('DynamoDB Record: %j', record.Dynamodb);

                    if (record.EventName === 'INSERT' || record.EventName === 'MODIFY') {
                        dynamo.putItem({
                            TableName: 'TABLENAME',
                            Item: record.Dynamodb.NewImage
                        }, nextRecord);
                    } else if (record.EventName === 'REMOVE') {
                        dynamo.deleteItem({
                            TableName: 'TABLENAME',
                            Key: record.Dynamodb.Keys
                        }, nextRecord);
                    }
                });
            });

            serial.awaitAll(nextId);
        });
    });

    q.awaitAll(function(err, data) {
        if (err) return context.fail(err);

        context.succeed('Processing successful');
    });
};

_.set(exports, 'helpers.actionsPerId', actionsPerId);

function actionsPerId(records) {
    return records.reduce(function(actionsPerId, action) {
        var id = JSON.stringify(action.Dynamodb.Keys);

        actionsPerId[id] = actionsPerId[id] || [];
        actionsPerId[id].push(action);
        return actionsPerId;
    }, {});
}
