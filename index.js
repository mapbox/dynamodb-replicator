var AWS = require('aws-sdk');
var queue = require('queue-async');
var streambot = require('streambot');

module.exports = replicate;
module.exports.streambot = streambot(replicate);

function replicate(event, callback) {
    console.log('Env: %s', JSON.stringify(process.env));

    var replicaConfig = { region: process.env.ReplicaRegion };
    if (process.env.ReplicaEndpoint) replicaConfig.endpoint = process.env.ReplicaEndpoint;
    var replica = new AWS.DynamoDB(replicaConfig);
    console.log(replicaConfig);

    var allRecords = event.Records.reduce(function(allRecords, action) {
        var id = JSON.stringify(action.Dynamodb.Keys);

        allRecords[id] = allRecords[id] || [];
        allRecords[id].push(action);
        return allRecords;
    }, {});

    var q = queue();

    Object.keys(allRecords).forEach(function(key) {
        var lastChange = allRecords[key].pop();
        q.defer(processChange, lastChange, replica);
    });

    q.awaitAll(callback);
}

function processChange(change, replica, callback) {
    var tableName;
    console.log('Processing %s to %j', change.EventName, change.Dynamodb.Keys);
    if (record.eventSourceARN.indexOf(process.env.PrimaryTables[0]) > -1) {
        tableName = process.env.PrimaryTables[0];
    } else {
        tableName = process.env.PrimaryTables[1];
    }
    console.log('putting to ' + tableName);

    var conditionalPutParams = {
        TableName: tableName,
        Item: record.Dynamodb.NewImage
    };

    if (record.EventName === 'INSERT') {
        var newKeys = Object.keys(record.Dynamodb.NewImage);

        conditionalPutParams.Expected = newKeys.reduce(function(memo, key) {
            memo[key] = {
                Exists: false
            };
            return memo;
        }, {});
        dynamo.putItem(conditionalPutParams, handlePutConflict);
    } else if (record.EventName === 'MODIFY') {
        var oldKeys = Object.keys(record.Dynamodb.OldImage);

        conditionalPutParams.Expected = oldKeys.reduce(function(memo, key) {
            memo[key] = {
                ComparisonOperator: 'EQ',
                Value: record.Dynamodb.OldImage[key]
            };
            return memo;
        }, {});
        dynamo.putItem(conditionalPutParams, handlePutConflict);
    } else if (record.EventName === 'REMOVE') {
        dynamo.deleteItem(conditionalPutParams, handleDeleteConflict);
    }

    function handlePutConflict(err, item) {
        if (err && err.code === 'ConditionalCheckFailedException') {
            // Check to see if the item has already been put, aka, if the existing item equals the new image
            dynamo.getItem({
                TableName: tableName,
                Key: record.Dynamodb.Keys
            }, function(err, item) {
                if (err) return callback(err);

                if (!itemsEqual(item, record.Dynamodb.NewImage)) {
                    // Put the newest item if the records differ, without any conditional checks
                    console.log('Item in conflict');
                    return dynamo.putItem({
                        TableName: tableName,
                        Item: record.Dynamodb.NewImage
                    }, callback);
                } else {
                    // Item has already been put, no need to update
                    console.log('Item already in correct state');
                    return callback();
                }
            });
        } else if (err) {
            return callback(err);
        }
        return callback();
    }

    function handleDeleteConflict(err, item) {
        if (err && err.code === 'ConditionalCheckFailedException') {
            // Check to see if the item has already been deleted
            dynamo.getItem({
                TableName: tableName,
                Key: record.Dynamodb.Keys
            }, function(err, item) {
                if (err) return callback(err);

                if (itemsEqual(item, {})) {
                    console.log('Item already deleted');
                    return callback();
                } else {
                    // Delete, this time without conditional
                    console.log('Item in conflict');
                    return dynamo.putItem({
                        TableName: tableName,
                        Key: record.Dynamodb.Keys
                    }, callback);
                }
            });
        } else if (err) {
            return callback(err);
        }
        return callback();
    }
}
