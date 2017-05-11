var AWS = require('aws-sdk');
var Dyno = require('dyno');
var queue = require('queue-async');
var crypto = require('crypto');
var https = require('https');

module.exports.replicate = replicate;
module.exports.backup = incrementalBackup;
module.exports.snapshot = require('./s3-snapshot');
module.exports.agent = new https.Agent({
    keepAlive: true,
    maxSockets: Math.ceil(require('os').cpus().length * 16),
    keepAliveMsecs: 60000
})

function replicate(event, context, callback) {
    var replicaConfig = {
        accessKeyId: process.env.ReplicaAccessKeyId || undefined,
        secretAccessKey: process.env.ReplicaSecretAccessKey || undefined,
        table: process.env.ReplicaTable,
        region: process.env.ReplicaRegion,
        maxRetries: 1000,
        httpOptions: {
            timeout: 2000,
            agent: module.exports.agent
        }
    };

    if (process.env.ReplicaEndpoint) replicaConfig.endpoint = process.env.ReplicaEndpoint;
    var replica = new Dyno(replicaConfig);

    var keyAttrs = Object.keys(event.Records[0].dynamodb.Keys);

    var filterer;
    if (process.env.TurnoverRole && process.env.TurnoverAt) {
        // Filterer function should return true if the record SHOULD be processed
        filterer = function(record) {
            var created = Number(record.dynamodb.ApproximateCreationDateTime + '000');
            var turnoverAt = Number(process.env.TurnoverAt);
            if (process.env.TurnoverRole === 'BEFORE') return created < turnoverAt;
            else if (process.env.TurnoverRole === 'AFTER') return created >= turnoverAt;
            else return true;
        };
    }

    var count = 0;
    var allRecords = event.Records.reduce(function(allRecords, change) {
        if (filterer && !filterer(change)) return allRecords;
        var id = JSON.stringify(change.dynamodb.Keys);
        allRecords[id] = allRecords[id] || [];
        allRecords[id].push(change);
        count++;
        return allRecords;
    }, {});

    if (count === 0) {
        console.log('No records replicated');
        return callback();
    }

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

    (function batchWrite(requestSet, attempts) {
        requestSet.forEach(function(req) {
            if (req) req.on('retry', function(res) {
                if (!res.error || !res.httpResponse || !res.httpResponse.headers) return;
                if (res.error.name === 'TimeoutError') res.error.retryable = true;
                console.log(
                    '[failed-request] %s | request-id: %s | crc32: %s | items: %j',
                    res.error.message,
                    res.httpResponse.headers['x-amzn-requestid'],
                    res.httpResponse.headers['x-amz-crc32'],
                    req.params.RequestItems[process.env.ReplicaTable].map(function(req) {
                        if (req.DeleteRequest) return req.DeleteRequest.Key;
                        if (req.PutRequest) return keyAttrs.reduce(function(key, k) {
                            key[k] = req.PutRequest.Item[k];
                            return key;
                        }, {});
                    })
                );
            });
        });

        requestSet.sendAll(100, function(errs, responses, unprocessed) {
            attempts++;

            if (errs) {
                var messages = errs
                    .filter(function(err) { return !!err; })
                    .map(function(err) { return err.message; })
                    .join(' | ');
                console.log('[error] %s', messages);
                return callback(errs);
            }

            if (unprocessed) {
                console.log('[retry] attempt %s contained unprocessed items', attempts);
                return setTimeout(batchWrite, Math.pow(2, attempts), unprocessed, attempts);
            }

            console.log('Replicated ' + count + ' records');
            callback();
        });
    })(replica.batchWriteItemRequests(params), 0);
}

function incrementalBackup(event, context, callback) {
    var params = {
        maxRetries: 1000,
        httpOptions: {
            timeout: 1000,
            agent: module.exports.agent
        }
    };

    if (process.env.BackupRegion) params.region = process.env.BackupRegion;

    var s3 = new AWS.S3(params);

    var filterer;
    if (process.env.TurnoverRole && process.env.TurnoverAt) {
        // Filterer function should return true if the record SHOULD be processed
        filterer = function(record) {
            var created = Number(record.dynamodb.ApproximateCreationDateTime + '000');
            var turnoverAt = Number(process.env.TurnoverAt);
            if (process.env.TurnoverRole === 'BEFORE') return created < turnoverAt;
            else if (process.env.TurnoverRole === 'AFTER') return created >= turnoverAt;
            else return true;
        };
    }

    var count = 0;
    var allRecords = event.Records.reduce(function(allRecords, action) {
        if (filterer && !filterer(action)) return allRecords;

        var id = JSON.stringify(action.dynamodb.Keys);

        allRecords[id] = allRecords[id] || [];
        allRecords[id].push(action);
        count++;
        return allRecords;
    }, {});

    if (count === 0) {
        console.log('No records backed up');
        return callback();
    }

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

                var table = change.eventSourceARN.split('/')[1];

                var params = {
                    Bucket: process.env.BackupBucket,
                    Key: [process.env.BackupPrefix, table, id].join('/')
                };

                var req = change.eventName === 'REMOVE' ? 'deleteObject' : 'putObject';
                if (req === 'putObject') params.Body = JSON.stringify(change.dynamodb.NewImage);

                s3[req](params, function(err) {
                    if (err) console.log(
                        '[error] %s | %s s3://%s/%s | %s',
                        JSON.stringify(change.dynamodb.Keys),
                        req, params.Bucket, params.Key,
                        err.message
                    );
                    next(err);
                }).on('retry', function(res) {
                    if (!res.error || !res.httpResponse || !res.httpResponse.headers) return;
                    if (res.error.name === 'TimeoutError') res.error.retryable = true;
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

        q.awaitAll(function(err) {
            if (err) return callback(err);
            console.log('Backed up ' + count + ' records')
            callback();
        });
    }
}
