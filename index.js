var AWS = require('aws-sdk');
var queue = require('queue-async');
var crypto = require('crypto');
var streambot = require('streambot');

module.exports = replicate;
module.exports.streambot = streambot(replicate);

function replicate(event, callback) {
    console.log('Env: %s', JSON.stringify(process.env));

    if (change.eventSourceARN.indexOf(process.env.PrimaryTables[0]) > -1) {
        var targetTable = process.env.PrimaryTables[1];
        var targetRegion = process.env.PrimaryRegions[1];
        var sourceTable = process.env.PrimaryTables[0];
        var sourceRegion = process.env.PrimaryRegions[0];
    } else {
        var targetTable = process.env.PrimaryTables[0];
        var targetRegion = process.env.PrimaryRegions[0];
        var sourceTable = process.env.PrimaryTables[1];
        var sourceRegion = process.env.PrimaryRegions[1];
    }
    console.log('source table: %s', sourceTable);
    console.log('target table: %s', targetTable);

    var targetConfig = { region: process.env.ReplicaRegion };
    var sourceConfig = { region: process.env.ReplicaRegion };

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
    console.log('Processing %s to %j', change.EventName, change.Dynamodb.Keys);

    var conditionalPutParams = {
        TableName: tableName,
        Item: change.Dynamodb.NewImage
    };

    if (change.EventName === 'INSERT') {
        var newKeys = Object.keys(change.Dynamodb.NewImage);

        conditionalPutParams.Expected = newKeys.reduce(function(memo, key) {
            memo[key] = {
                Exists: false
            };
            return memo;
        }, {});
        dynamo.putItem(conditionalPutParams, handlePutConflict);
    } else if (change.EventName === 'MODIFY') {
        var oldKeys = Object.keys(change.Dynamodb.OldImage);

        conditionalPutParams.Expected = oldKeys.reduce(function(memo, key) {
            memo[key] = {
                ComparisonOperator: 'EQ',
                Value: change.Dynamodb.OldImage[key]
            };
            return memo;
        }, {});
        dynamo.putItem(conditionalPutParams, handlePutConflict);
    } else if (change.EventName === 'REMOVE') {
        dynamo.deleteItem(conditionalPutParams, handleDeleteConflict);
    }

    function handlePutConflict(err, item) {
        if (err && err.code === 'ConditionalCheckFailedException') {
            // Check to see if the item has already been put, aka, if the existing item equals the new image
            dynamo.getItem({
                TableName: tableName,
                Key: change.Dynamodb.Keys
            }, function(err, targetItem) {
                if (err) return callback(err);

                if (itemsEqual(targetItem, change.Dynamodb.NewImage)) {
                    // Item has already been put, no need to update
                    console.log('Item already in correct state');
                    return callback();
                } else {
                    // If the records differ, compare modified, and resolve to the most recent
                    console.log('Item in conflict');
                    if (targetItem.modified > change.Dynamodb.NewImage.modified) {
                        // Existing item is newer, write it to the source database
                        return dynamo.putItem({
                            TableName: sourceTable,
                            Item: targetItem
                        }, callback);
                    } else if (targetItem.modified < change.Dynamodb.NewImage.modified) {
                        // Update from source is newer, write to the target database
                        return dynamo.putItem({
                            TableName: targetTable,
                            Item: change.Dynamodb.NewImage
                        }, callback);
                    } else { // modified is equal, resolve with the record with the alphabetically first md5 hash
                        var md5target = crypto.createHash('md5');
                        md5target.update(JSON.stringify(targetItem), 'utf8');
                        var targetHash = md5target.digest('hex');
                        var md5source = crypto.createHash('md5');
                        md5source.update(JSON.stringify(change.Dynamodb.NewImage), 'utf8');
                        var source = md5source.digest('hex');
                        if (targetHash > sourceHash) {
                            return dynamo.putItem({
                                TableName: sourceTable,
                                Item: targetItem
                            }, callback);
                        } else {
                            return dynamo.putItem({
                                TableName: targetTable,
                                Item: change.Dynamodb.NewImage
                            }, callback);
                        }
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
                Key: change.Dynamodb.Keys
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
                        Key: change.Dynamodb.Keys
                    }, callback);
                }
            });
        } else if (err) {
            return callback(err);
        }
        return callback();
    }
}
