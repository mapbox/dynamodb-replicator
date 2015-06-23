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

    var allRecords = event.Records.reduce(function(allRecords, action) {
        var id = JSON.stringify(action.Dynamodb.Keys);

        allRecords[id] = allRecords[id] || [];
        allRecords[id].push(action);
        return allRecords;
    }, {});

    var q = queue();

    Object.keys(allRecords).forEach(function(key) {
        q.defer(processRecord, allRecords[key], replica);
    });

    q.awaitAll(callback);
}

 function processRecord(changes, replica, callback) {
    var thisRecord = queue(1);

    changes.forEach(function(change) {
        thisRecord.defer(processChange, change, replica);
    });

    thisRecord.awaitAll(callback);
}

function processChange(record, replica, callback) {
    console.log('Processing %s to %j', record.EventName, record.Dynamodb.Keys);

    if (record.EventName === 'INSERT' || record.EventName === 'MODIFY') {
        replica.putItem({
            TableName: process.env.ReplicaTable,
            Item: record.Dynamodb.NewImage
        }, callback);
    } else if (record.EventName === 'REMOVE') {
        replica.deleteItem({
            TableName: process.env.ReplicaTable,
            Key: record.Dynamodb.Keys
        }, callback);
    }
}
