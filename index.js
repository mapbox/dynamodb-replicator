var AWS = require('aws-sdk');
var queue = require('queue-async');
var streambot = require('streambot');
var crypto = require('crypto');
var s3 = new AWS.S3();

module.exports.replicate = replicate;
module.exports.streambotReplicate = streambot(replicate);
module.exports.backup = incrementalBackup;
module.exports.streambotBackup = streambot(incrementalBackup);

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

function incrementalBackup(event, callback) {
    console.log('Env: %s', JSON.stringify(process.env));

    var allRecords = event.Records.reduce(function(allRecords, action) {
        var id = JSON.stringify(action.dynamodb.Keys);

        allRecords[id] = allRecords[id] || [];
        allRecords[id].push(action);
        return allRecords;
    }, {});

    var q = queue();

    Object.keys(allRecords).forEach(function(key) {
        q.defer(backupRecord, allRecords[key]);
    });

    q.awaitAll(function(err) {
        if (err) throw err;
        callback();
    });
}

function backupRecord(changes, callback) {
    var q = queue(1);

    changes.forEach(function(change) {
        q.defer(function(next) {
            var id = crypto.createHash('md5')
                .update(JSON.stringify(change.dynamodb.Keys))
                .digest('hex');

            var table = change.eventSourceARN.split('/')[1];

            var params = {
                Bucket: process.env.BackupBucket,
                Key: [process.env.BackupPrefix, table, id].join('/')
            };

            console.log('Backing up %s to %j', change.eventName, change.dynamodb.Keys);

            var req = change.eventName === 'REMOVE' ? 'deleteObject' : 'putObject';
            if (req === 'putObject') params.Body = JSON.stringify(change.dynamodb.NewImage);

            s3[req](params, next);
        });
    });

    q.awaitAll(callback);
}
