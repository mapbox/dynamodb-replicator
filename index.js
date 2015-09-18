var AWS = require('aws-sdk');
var queue = require('queue-async');
var streambot = require('streambot');
var crypto = require('crypto');

module.exports.replicate = replicate;
module.exports.streambotReplicate = streambot(replicate);
module.exports.backup = incrementalBackup;
module.exports.streambotBackup = streambot(incrementalBackup);

function printRemaining(events, first) {
    var msg = '--> %s events remaining';
    if (first) msg = msg.replace('remaining', 'total');

    console.log(msg, Object.keys(events).length);
    Object.keys(events).forEach(function(hash) {
        console.log('%s: %s %s', hash, events[hash].action, events[hash].key);
    });
}

function replicate(event, callback) {
    var replicaConfig = {
        region: process.env.ReplicaRegion,
        maxRetries: 1000,
        httpOptions: {
            timeout: 500,
            agent: streambot.agent
        }
    };
    if (process.env.ReplicaEndpoint) replicaConfig.endpoint = process.env.ReplicaEndpoint;
    var replica = new AWS.DynamoDB(replicaConfig);

    var events = {};
    var allRecords = event.Records.reduce(function(allRecords, action) {
        var id = JSON.stringify(action.dynamodb.Keys);
        allRecords[id] = allRecords[id] || [];
        allRecords[id].push(action);
        return allRecords;
    }, {});

    var q = queue();
    var batchChanges = [];
    Object.keys(allRecords).forEach(function(key) {
        var change = allRecords[key].pop();

        var hash = crypto.createHash('md5').update([
            change.eventName,
            JSON.stringify(change.dynamodb.Keys),
            JSON.stringify(change.dynamodb.NewImage)
        ].join('')).digest('hex');
        events[hash] = { key: key, action: change.eventName };

        batchChanges.push(change);
        if (batchChanges.length === 25) {
            q.defer(processChange, batchChanges, replica);
            batchChanges = [];
        }
    });
    if (batchChanges.length > 0) q.defer(processChange, batchChanges, replica);

    printRemaining(events, true);

    q.awaitAll(callback);

    function processChange(changes, replica, next) {
        var requests = { RequestItems:{} };
        requests.RequestItems[process.env.ReplicaTable] = [];

        changes.forEach(function(change) {
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

                requests.RequestItems[process.env.ReplicaTable].push({
                    PutRequest: {
                        Item: newItem
                    }
                });
            } else if (change.eventName === 'REMOVE') {
                requests.RequestItems[process.env.ReplicaTable].push({
                    DeleteRequest: {
                        Key: change.dynamodb.Keys
                    }
                });
            }
        });

        replica.batchWriteItem(requests, function(err){
            if (err) return next(err);

            changes.forEach(function(change){
                var hash = crypto.createHash('md5').update([
                    change.eventName,
                    JSON.stringify(change.dynamodb.Keys),
                    JSON.stringify(change.dynamodb.NewImage)
                ].join('')).digest('hex');
                delete events[hash];
            });
            printRemaining(events);
            next();
        });
    }
}

function incrementalBackup(event, callback) {
    var events = {};
    var allRecords = event.Records.reduce(function(allRecords, action) {
        var id = JSON.stringify(action.dynamodb.Keys);
        var hash = crypto.createHash('md5')
            .update([
                action.eventName,
                JSON.stringify(action.dynamodb.Keys),
                JSON.stringify(action.dynamodb.NewImage)
            ].join(''))
            .digest('hex');
        events[hash] = { key: id, action: action.eventName };

        allRecords[id] = allRecords[id] || [];
        allRecords[id].push(action);
        return allRecords;
    }, {});

    var s3 = new AWS.S3({
        maxRetries: 1000,
        httpOptions: {
            timeout: 1000,
            agent: streambot.agent
        }
    });

    printRemaining(events, true);

    var q = queue();

    Object.keys(allRecords).forEach(function(key) {
        q.defer(backupRecord, allRecords[key]);
    });

    q.awaitAll(function(err) {
        if (err) throw err;
        callback();
    });

    function backupRecord(changes, callback) {
        var q = queue(1);

        changes.forEach(function(change) {
            q.defer(function(next) {
                var id = crypto.createHash('md5')
                    .update(JSON.stringify(change.dynamodb.Keys))
                    .digest('hex');
                var hash = crypto.createHash('md5')
                    .update([
                        change.eventName,
                        JSON.stringify(change.dynamodb.Keys),
                        JSON.stringify(change.dynamodb.NewImage)
                    ].join(''))
                    .digest('hex');

                var table = change.eventSourceARN.split('/')[1];

                var params = {
                    Bucket: process.env.BackupBucket,
                    Key: [process.env.BackupPrefix, table, id].join('/')
                };

                var req = change.eventName === 'REMOVE' ? 'deleteObject' : 'putObject';
                if (req === 'putObject') params.Body = JSON.stringify(change.dynamodb.NewImage);

                s3[req](params, function(err) {
                    if (err) return next(err);
                    delete events[hash];
                    printRemaining(events);
                    next();
                });
            });
        });

        q.awaitAll(callback);
    }
}
