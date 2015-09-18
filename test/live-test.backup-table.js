var test = require('tape');
var setup = require('./setup')(true);
var exec = require('child_process').exec;
var path = require('path');
var crypto = require('crypto');
var AWS = require('aws-sdk');
var queue = require('queue-async');

var starttime = (new Date()).toISOString();

test('setup', setup.setup);

test('backup-table shell script', function(assert) {
    var jobid = crypto.randomBytes(4).toString('hex');

    var cmd = [
        path.resolve(__dirname, '..', 'bin', 'backup-table.js'),
        'us-east-1/' + setup.config.primary.table,
        's3://mapbox/dynamodb-replicator/test',
        '--jobid', jobid,
        '--segment 0',
        '--segments 1',
        '--metric Mapbox'
    ].join(' ');

    exec(cmd, function(err) {
        assert.ifError(err, 'success');

        var s3 = new AWS.S3();
        var cw = new AWS.CloudWatch({ region: 'us-east-1' });

        console.log('Waiting 60s for CW to land...');

        setTimeout(function() {
            queue()
                .defer(function(next) {
                    s3.getObject({
                        Bucket: 'mapbox',
                        Key: 'dynamodb-replicator/test/' + jobid + '/0'
                    }, function(err, data) {
                        assert.ifError(err, 'S3 getObject success');
                        assert.ok(data.Body, 'Backup written to s3');
                        next();
                    });
                })
                .defer(function(next) {
                    cw.getMetricStatistics({
                        Namespace: 'Mapbox',
                        Dimensions: [
                            {
                                Name: 'TableName',
                                Value: setup.config.primary.table
                            }
                        ],
                        MetricName: 'BackupSize',
                        StartTime: starttime,
                        EndTime: (new Date()).toISOString(),
                        Period: 60,
                        Statistics: ['Sum']
                    }, function(err, data) {
                        assert.ifError(err, 'CW BackupSize success');
                        if (!data.Datapoints || !data.Datapoints.length) {
                            assert.fail('No CW data found');
                            return next();
                        }
                        assert.equal(data.Datapoints.length, 1, 'BackupSize put to CW');
                        assert.equal(data.Datapoints[0].Sum, 98, 'Correct BackupSize value on CW');
                        next();
                    });
                })
                .defer(function(next) {
                    cw.getMetricStatistics({
                        Namespace: 'Mapbox',
                        Dimensions: [
                            {
                                Name: 'TableName',
                                Value: setup.config.primary.table
                            }
                        ],
                        MetricName: 'BackupRecordCount',
                        StartTime: starttime,
                        EndTime: (new Date()).toISOString(),
                        Period: 60,
                        Statistics: ['Sum']
                    }, function(err, data) {
                        assert.ifError(err, 'CW BackupRecordCount success');
                        if (!data.Datapoints || !data.Datapoints.length) {
                            assert.fail('No CW data found');
                            return next();
                        }
                        assert.equal(data.Datapoints.length, 1, 'BackupRecordCount put to CW');
                        assert.equal(data.Datapoints[0].Sum, 3, 'Correct BackupRecordCount value on CW');
                        next();
                    });
                })
                .await(function() {
                    assert.end();
                });
        }, 60000);
    });
});

test('teardown', setup.teardown);
