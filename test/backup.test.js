var test = require('tape');
var setup = require('./setup')(process.env.LIVE_TEST);
var backup = require('../backup');
var _ = require('underscore');
var crypto = require('crypto');
var AWS = require('aws-sdk');
var s3 = new AWS.S3();
var queue = require('queue-async');

test('setup', setup.setup);
test('backup: one segment', function(assert) {
    var config = {
        backup: {
            bucket: 'mapbox',
            prefix: 'dynamodb-replicator/test',
            jobid: crypto.randomBytes(4).toString('hex')
        },
        table: setup.config.primary.table,
        region: setup.config.primary.region,
        accessKeyId: setup.config.primary.accessKeyId,
        secretAccessKey: setup.config.primary.secretAccessKey,
        endpoint: setup.config.primary.endpoint
    };

    backup(config, function(err) {
        assert.ifError(err, 'backup completed');
        if (err) return assert.end();

        s3.getObject({
            Bucket: 'mapbox',
            Key: [config.backup.prefix, config.backup.jobid, '0'].join('/')
        }, function(err, data) {
            assert.ifError(err, 'retrieved backup from S3');
            if (err) return assert.end();

            assert.ok(data.Body, 'file has content');

            data = data.Body.toString().trim().split('\n');
            assert.deepEqual(data, [
                '{"hash":"hash1","range":"range1","other":1}',
                '{"hash":"hash1","range":"range2","other":2}',
                '{"hash":"hash1","range":"range4","other":"base64:aGVsbG8gd29ybGQ="}'
            ], 'expected data backed up to S3');

            assert.end();
        });
    });
});
test('teardown', setup.teardown);

test('setup', setup.setup);
test('load many', function(assert) {
    var records = _.range(1000).map(function() {
        return {
            hash: crypto.randomBytes(8).toString('hex'),
            range: crypto.randomBytes(8).toString('hex'),
            other: crypto.randomBytes(8)
        };
    });

    setup.dynos.primary.putItems(records, function(err) {
        if (err) throw err;
        assert.end();
    });
});
test('backup: parallel', function(assert) {
    var config = {
        backup: {
            bucket: 'mapbox',
            prefix: 'dynamodb-replicator/test',
            jobid: crypto.randomBytes(4).toString('hex')
        },
        table: setup.config.primary.table,
        region: setup.config.primary.region,
        accessKeyId: setup.config.primary.accessKeyId,
        secretAccessKey: setup.config.primary.secretAccessKey,
        endpoint: setup.config.primary.endpoint,
        segments: 2
    };

    var firstConfig = _({ segment: 0 }).extend(config);
    var secondConfig = _({ segment: 1 }).extend(config);
    var firstKey = [config.backup.prefix, config.backup.jobid, firstConfig.segment].join('/');
    var secondKey = [config.backup.prefix, config.backup.jobid, secondConfig.segment].join('/');

    queue(1)
        .defer(backup, firstConfig)
        .defer(backup, secondConfig)
        .defer(s3.getObject.bind(s3), { Bucket: 'mapbox', Key: firstKey })
        .defer(s3.getObject.bind(s3), { Bucket: 'mapbox', Key: secondKey })
        .awaitAll(function(err, results) {
            assert.ifError(err, 'all requests completed');
            if (err) return assert.end();

            var data = results.slice(2).map(function(s3result) {
                return s3result.Body.toString().trim().split('\n');
            });

            assert.equal(data[0].length + data[1].length, 1003, 'backed up all records');
            assert.end();
        });
});
test('teardown', setup.teardown);
