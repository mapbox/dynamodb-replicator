var test = require('tape');
var dynamodb = require('@mapbox/dynamodb-test')(test, 'dynamodb-replicator', require('./table.json'), 'us-east-1');
var exec = require('child_process').exec;
var path = require('path');
var crypto = require('crypto');
var { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
var { CloudWatchClient, GetMetricStatisticsCommand } = require('@aws-sdk/client-cloudwatch');
var queue = require('queue-async');

var primaryItems = [
    {hash: 'hash1', range: 'range1', other:1},
    {hash: 'hash1', range: 'range2', other:2},
    {hash: 'hash1', range: 'range4', other: new Buffer('hello world')}
];

var starttime = (new Date()).toISOString();

dynamodb.start();

dynamodb.test('backup-table shell script', primaryItems, function(assert) {
    var jobid = crypto.randomBytes(4).toString('hex');

    var cmd = [
        path.resolve(__dirname, '..', 'bin', 'backup-table.js'),
        'us-east-1/' + dynamodb.tableName,
        's3://mapbox/dynamodb-replicator/test',
        '--jobid', jobid,
        '--segment 0',
        '--segments 1',
        '--metric Mapbox'
    ].join(' ');

    exec(cmd, function(err) {
        assert.ifError(err, 'success');
        var s3Client = new S3Client({
            region: 'us-east-1',
            credentials: {
                accessKeyId: 'fake',
                secretAccessKey: 'fake'
            }
        });
        var cwClient = new CloudWatchClient({
            region: 'us-east-1',
            credentials: {
                accessKeyId: 'fake',
                secretAccessKey: 'fake'
            }
        });

        console.log('Waiting 60s for CW to land...');

        setTimeout(function() {
            queue()
                .defer(function(next) {
                    s3Client.send(new GetObjectCommand({
                        Bucket: 'mapbox',
                        Key: 'dynamodb-replicator/test/' + jobid + '/0'
                    })).then(function(data) {
                        assert.ok(data.Body, 'Backup written to s3');
                        next();
                    }).catch(function(err) {
                        assert.ifError(err, 'S3 getObject success');
                        next(err);
                    });
                })
                .defer(function(next) {
                    cwClient.send(new GetMetricStatisticsCommand({
                        Namespace: 'Mapbox',
                        Dimensions: [
                            {
                                Name: 'TableName',
                                Value: dynamodb.tableName
                            }
                        ],
                        MetricName: 'BackupSize',
                        StartTime: starttime,
                        EndTime: (new Date()).toISOString(),
                        Period: 60,
                        Statistics: ['Sum']
                    })).then(function(data) {
                        if (!data.Datapoints || !data.Datapoints.length) {
                            assert.fail('No CW data found');
                            return next();
                        }
                        assert.equal(data.Datapoints.length, 1, 'BackupSize put to CW');
                        assert.equal(data.Datapoints[0].Sum, 101, 'Correct BackupSize value on CW');
                        next();
                    }).catch(function(err) {
                        assert.ifError(err, 'CW BackupSize success');
                        next(err);
                    });
                })
                .defer(function(next) {
                    cwClient.send(new GetMetricStatisticsCommand({
                        Namespace: 'Mapbox',
                        Dimensions: [
                            {
                                Name: 'TableName',
                                Value: dynamodb.tableName
                            }
                        ],
                        MetricName: 'BackupRecordCount',
                        StartTime: starttime,
                        EndTime: (new Date()).toISOString(),
                        Period: 60,
                        Statistics: ['Sum']
                    })).then(function(data) {
                        if (!data.Datapoints || !data.Datapoints.length) {
                            assert.fail('No CW data found');
                            return next();
                        }
                        assert.equal(data.Datapoints.length, 1, 'BackupRecordCount put to CW');
                        assert.equal(data.Datapoints[0].Sum, 3, 'Correct BackupRecordCount value on CW');
                        next();
                    }).catch(function(err) {
                        assert.ifError(err, 'CW BackupRecordCount success');
                        next(err);
                    });
                })
                .await(function() {
                    assert.end();
                });
        }, 60000);
    });
});

dynamodb.delete();
