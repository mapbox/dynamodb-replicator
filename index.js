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
        var id = JSON.stringify(action.dynamodb.Keys);

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
    console.log('Processing %s to %j', change.eventName, change.dynamodb.Keys);

    if (change.eventName === 'INSERT' || change.eventName === 'MODIFY') {
        var newItem = (function decodeBuffers(obj) {
            if (typeof obj !== 'object') return obj;

            return Object.keys(obj).reduce(function(newObj, key) {
                var value = obj[key];

                if (key === 'B' && typeof value === 'string')
                    newObj[key] = new Buffer(value, 'base64');

                else if (key === 'BS' && Array.isArray(value))
                    newObj[key] = value.map(function(encoded) {
                        return new Buffer(encoded, 'base64');
                    });

                else if (Array.isArray(value))
                    newObj[key] = value.map(decodeBuffers);

                else if (typeof value === 'object')
                    newObj[key] = decodeBuffers(value);

                else
                    newObj[key] = value;

                return newObj;
            }, {});
        })(change.dynamodb.NewImage);

        replica.putItem({
            TableName: process.env.ReplicaTable,
            Item: newItem
        }, callback);
    } else if (change.eventName === 'REMOVE') {
        replica.deleteItem({
            TableName: process.env.ReplicaTable,
            Key: change.dynamodb.Keys
        }, callback);
    }
}
