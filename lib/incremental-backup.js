var AWS = require('aws-sdk');
var d3 = require('d3-queue');
var crypto = require('crypto');
var https = require('https');

module.exports = function incrementalBackup(event, context, callback) {
    var params = {
        maxRetries: 1000,
        httpOptions: {
            timeout: 1000,
            agent: new https.Agent({
                keepAlive: true,
                maxSockets: Math.ceil(require('os').cpus().length * 16),
                keepAliveMsecs: 60000
            })
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

    var q = d3.queue();

    Object.keys(allRecords).forEach(function(key) {
        q.defer(backupRecord, allRecords[key]);
    });

    q.awaitAll(function(err) {
        if (err) throw err;
        callback();
    });

    function backupRecord(changes, callback) {
        var q = d3.queue(1);

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
