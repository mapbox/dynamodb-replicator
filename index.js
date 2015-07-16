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
    console.log('Processing %s to %j', change.EventName, change.Dynamodb.Keys);

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
}
