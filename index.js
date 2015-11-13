var AWS = require('aws-sdk');
var Dyno = require('dyno');
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
        table: process.env.ReplicaTable,
        region: process.env.ReplicaRegion,
        maxRetries: 1000,
        httpOptions: {
            timeout: 500,
            agent: streambot.agent
        }
    };
    if (process.env.ReplicaEndpoint) replicaConfig.endpoint = process.env.ReplicaEndpoint;
    var replica = new Dyno(replicaConfig);

    var allRecords = event.Records.reduce(function(allRecords, change) {
        var id = JSON.stringify(change.dynamodb.Keys);
        allRecords[id] = allRecords[id] || [];
        allRecords[id].push(change);
        return allRecords;
    }, {});

    var params = { RequestItems: {} };
    params.RequestItems[process.env.ReplicaTable] = Object.keys(allRecords).map(function(key) {
        var change = allRecords[key].pop();
        if (change.eventName === 'INSERT' || change.eventName === 'MODIFY') {
            return {
                PutRequest: { Item: Dyno.deserialize(JSON.stringify(change.dynamodb.NewImage)) }
            };
        } else if (change.eventName === 'REMOVE') {
            return {
                DeleteRequest: { Key: Dyno.deserialize(JSON.stringify(change.dynamodb.Keys)) }
            }
        }
    });

    var attempts = 0;
    (function batchWrite(requestSet) {
        requestSet.forEach(function(req) {
            if (!req._listener) req.on('retry', function(res) {
                if (!res.error || !res.httpResponse || !res.httpResponse.headers) return;
                console.log(
                    '[failed-request] request-id: %s | id-2: %s | params: %j',
                    res.httpResponse.headers['x-amz-request-id'],
                    res.httpResponse.headers['x-amz-id-2'],
                    req.params
                );
            });
            req._listener = true;
        });

        requestSet.sendAll(100, function(errs, responses, unprocessed) {
            attempts++;
            if (!errs && !unprocessed) return callback();

            var retry = unprocessed ? unprocessed : requestSet;

            if (unprocessed && errs) errs.forEach(function(err, i) {
                if (err) unprocessed.push(requestSet[i]);
            });

            else if (errs) errs.forEach(function(err, i) {
                if (!err) requestSet[i] = null;
            });

            console.log('[retry] attempt %s contained errors and/or unprocessed items', attempts);
            return setTimeout(batchWrite, Math.pow(2, attempts), retry);
        });
    })(replica.batchWriteItemRequests(params));
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

    var params = {
        maxRetries: 1000,
        httpOptions: {
            timeout: 1000,
            agent: streambot.agent
        }
    };

    if (process.env.BackupRegion) params.region = process.env.BackupRegion;

    var s3 = new AWS.S3(params);

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
                }).on('retry', function(res) {
                    if (!res.error || !res.httpResponse || !res.httpResponse.headers) return;
                    console.log(
                        '[failed-request] request-id: %s | id-2: %s | %s s3://%s/%s | %s',
                        res.httpResponse.headers['x-amz-request-id'],
                        res.httpResponse.headers['x-amz-id-2'],
                        req, params.Bucket, params.Key,
                        res.error
                    );
                });
            });
        });

        q.awaitAll(callback);
    }
}
