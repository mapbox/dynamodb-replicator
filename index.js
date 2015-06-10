var AWS = require('aws-sdk');
var queue = require('queue-async');
var _ = require('underscore');
var streambot = require('streambot');

module.exports = replicate;
module.exports.streambot = streambot(replicate);

function replicate(records, callback) {
    var replicaConfig = {
        region: process.env.ReplicaRegion,
        endpoint: process.env.ReplicaEndpoint
    };
    /*
    var replicaConfig = {
        region: 'us-east-1',
        endpoint: 'https://preview-dynamodb.us-east-1.amazonaws.com'
    };
    */

    var replica = new AWS.DynamoDB(replicaConfig);

    var recordsById = actionsPerId(records);

    var q = queue();

    _(recordsById).each(function(records, id) {
        q.defer(function(nextId) {
            var serial = queue(1);

            records.forEach(function(record) {
                serial.defer(function(nextRecord) {
                    streambot.log.info('Processing %j', record.Dynamodb.Keys);

                    if (record.EventName === 'INSERT' || record.EventName === 'MODIFY') {
                        replica.putItem({
                            TableName: process.env.ReplicaTable,
                            Item: record.Dynamodb.NewImage
                        }, nextRecord);
                    } else if (record.EventName === 'REMOVE') {
                        replica.deleteItem({
                            TableName: process.env.ReplicaTable,
                            Key: record.Dynamodb.Keys
                        }, nextRecord);
                    }
                });
            });

            serial.awaitAll(nextId);
        });
    });

    q.awaitAll(callback);
};

module.exports.helpers = {
    actionsPerId: actionsPerId
};

function actionsPerId(records) {
    return records.reduce(function(actionsPerId, action) {
        var id = JSON.stringify(action.Dynamodb.Keys);

        actionsPerId[id] = actionsPerId[id] || [];
        actionsPerId[id].push(action);
        return actionsPerId;
    }, {});
}
