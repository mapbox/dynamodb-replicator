var test = require('tape');
var setup = require('./setup')(process.env.LIVE_TEST);
var backup = require('../backup');
var _ = require('underscore');
var crypto = require('crypto');
var AWS = require('aws-sdk');
var s3 = new AWS.S3();
var queue = require('queue-async');
var zlib = require('zlib');

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

    backup(config, function(err, details) {
        assert.ifError(err, 'backup completed');
        if (err) return assert.end();

        assert.equal(details.count, 3, 'reported 3 records');
        assert.equal(details.size, 110, 'reported 110 bytes');

        s3.getObject({
            Bucket: 'mapbox',
            Key: [config.backup.prefix, config.backup.jobid, '0'].join('/')
        }, function(err, data) {
            assert.ifError(err, 'retrieved backup from S3');
            if (err) return assert.end();

            assert.ok(data.Body, 'file has content');

            zlib.gunzip(data.Body, function(err, data) {
                assert.ifError(err, 'gzipped backup');
                data = data.toString().trim().split('\n');
                assert.deepEqual(data, [
                    '{"hash":{"S":"hash1"},"range":{"S":"range1"},"other":{"N":"1"}}',
                    '{"hash":{"S":"hash1"},"range":{"S":"range2"},"other":{"N":"2"}}',
                    '{"hash":{"S":"hash1"},"range":{"S":"range4"},"other":{"B":[104,101,108,108,111,32,119,111,114,108,100]}}'
                ], 'expected data backed up to S3');

                assert.end();
            });
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

            assert.equal(results[0].count + results[1].count, 1003, 'reported 1003 records');

            var s3results = results.slice(2);
            zlib.gunzip(s3results[0].Body, function(err, first) {
                assert.ifError(err, 'gzipped backup');
                zlib.gunzip(s3results[1].Body, function(err, second) {
                    assert.ifError(err, 'gzipped backup');
                    first = first.toString().trim().split('\n');
                    second = second.toString().trim().split('\n');
                    assert.equal(first.length + second.length, 1003, 'backed up all records');
                    assert.end();
                });
            });
        });
});
test('teardown', setup.teardown);
