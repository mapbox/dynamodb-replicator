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

    dynamo.getItem({
        TableName: tableName,
        Key: record.Dynamodb.Keys
    }, function(err, item) {
        if (err) return context.fail(err);
        if (Object.keys(item).length === 0) {
            // The object does not exist in the target table
            // Needed because !{} !== false
            item = false;
        }

        if (change.EventName === 'INSERT' || change.EventName === 'MODIFY') {
            replica.putItem({
                TableName: process.env.ReplicaTable,
                Item: change.Dynamodb.NewImage
            }, callback);
        } else if (change.EventName === 'REMOVE') {
            replica.deleteItem({
                TableName: process.env.ReplicaTable,
                Key: change.Dynamodb.Keys
            }, callback);
        }
        if (record.EventName === 'INSERT') {
            if (!item) {
                // Item doesn't exist, go ahead and put
                console.log('Item doesn\'t exist, go ahead and put');
                dynamo.putItem({
                    TableName: tableName,
                    Item: record.Dynamodb.NewImage,
                }, callback);
            } else if (itemsEqual(item, record.NewImage)) {
                // Item already exists, do not put
                console.log('Item already exists, do not put');
                callback(null);
            } else {
                // Items are in conflict :(
                console.log('Items are in conflict :(');
                console.log(item);
                console.log(record.NewImage);
                callback(null);
            }
        } else if (record.EventName === 'REMOVE') {
            if (!item) {
                // Item has already been removed, do nothing
                console.log('Item has already been removed, do nothing');
                callback(null);
            } else if (itemsEqual(item, record.OldImage)) {
                // Items are equal, go ahead and delete
                console.log('Items are equal, go ahead and delete');
                dynamo.deleteItem({
                    TableName: tableName,
                    Key: record.Dynamodb.Keys
                }, callback);
            } else {
                // Items are in conflict :(
                console.log('Items are in conflict :(');
                console.log(item);
                console.log(record.OldImage);
                callback(null);
            }
        } else if (record.EventName === 'MODIFY') {
            if (!item) {
                // Item has already been removed, Items are in conflict :(
                console.log('Items are in conflict :(');
                callback(null);
            } else if (itemsEqual(item, record.OldImage)) {
                // Items is ready to be updated to the new state
                console.log('Items is ready to be updated to the new state');
                dynamo.putItem({
                    TableName: tableName,
                    Item: record.Dynamodb.NewImage,
                }, callback);
            } else if (itemsEqual(item, record.NewImage)) {
                // Items has already been updated
                console.log('Items has already been updated');
                callback(null);
            } else {
                // Items are in conflict :(
                console.log('Items are in conflict :(');
                console.log(item);
                console.log(record.OldImage);
                console.log(record.NewImage);
                callback(null);
            }
        }
    });
}

function itemsEqual(itema, itemb) {
    if (typeof itema === 'object' && typeof itemb === 'object') {
        return Object.keys(itema).every(function(key) {
            return itemsEqual(itema[key], itemb[key]);
        });
    } else {
        return itema === itemb;
    }
}
