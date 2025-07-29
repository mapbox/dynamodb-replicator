var test = require('tape');
var dynamodb = require('@mapbox/dynamodb-test')(test, 'dynamodb-replicator', require('./table.json'))
var backup = require('../backup');
var _ = require('underscore');
var crypto = require('crypto');
var { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
var s3Client = new S3Client({
    region: 'us-east-1',
    credentials: {
        accessKeyId: 'fake',
        secretAccessKey: 'fake'
    }
});
var queue = require('queue-async');
var zlib = require('zlib');

var primaryItems = [
    {hash: 'hash1', range: 'range1', other:1},
    {hash: 'hash1', range: 'range2', other:2},
    {hash: 'hash1', range: 'range4', other: new Buffer('hello world')}
];

var records = _.range(1000).map(function() {
    return {
        hash: crypto.randomBytes(8).toString('hex'),
        range: crypto.randomBytes(8).toString('hex'),
        other: crypto.randomBytes(8)
    };
});

dynamodb.start();

dynamodb.test('backup: one segment', primaryItems, function(assert) {
    var config = {
        backup: {
            bucket: 'mapbox',
            prefix: 'dynamodb-replicator/test',
            jobid: crypto.randomBytes(4).toString('hex')
        },
        table: dynamodb.tableName,
        region: 'us-east-1',
        accessKeyId: 'fake',
        secretAccessKey: 'fake',
        endpoint: 'http://localhost:4567'
    };

    backup(config, function(err, details) {
        assert.ifError(err, 'backup completed');
        if (err) return assert.end();

        assert.equal(details.count, 3, 'reported 3 records');
        assert.equal(details.size, 101, 'reported 101 bytes');

        s3Client.send(new GetObjectCommand({
            Bucket: 'mapbox',
            Key: [config.backup.prefix, config.backup.jobid, '0'].join('/')
        })).then(function(data) {
            assert.ok(data.Body, 'file has content');

            zlib.gunzip(data.Body, function(err, data) {
                assert.ifError(err, 'gzipped backup');
                data = data.toString().trim().split('\n');
                assert.deepEqual(data, [
                    '{"hash":{"S":"hash1"},"range":{"S":"range1"},"other":{"N":"1"}}',
                    '{"hash":{"S":"hash1"},"range":{"S":"range2"},"other":{"N":"2"}}',
                    '{"hash":{"S":"hash1"},"range":{"S":"range4"},"other":{"B":"aGVsbG8gd29ybGQ="}}'
                ], 'expected data backed up to S3');

                assert.end();
            });
        });
    });
});

dynamodb.test('backup: parallel', records, function(assert) {
    var config = {
        backup: {
            bucket: 'mapbox',
            prefix: 'dynamodb-replicator/test',
            jobid: crypto.randomBytes(4).toString('hex')
        },
        table: dynamodb.tableName,
        region: 'us-east-1',
        accessKeyId: 'fake',
        secretAccessKey: 'fake',
        endpoint: 'http://localhost:4567',
        segments: 2
    };

    var firstConfig = _({ segment: 0 }).extend(config);
    var secondConfig = _({ segment: 1 }).extend(config);
    var firstKey = [config.backup.prefix, config.backup.jobid, firstConfig.segment].join('/');
    var secondKey = [config.backup.prefix, config.backup.jobid, secondConfig.segment].join('/');

    queue(1)
        .defer(backup, firstConfig)
        .defer(backup, secondConfig)
        .defer(function(cb) {
            s3Client.send(new GetObjectCommand({ Bucket: 'mapbox', Key: firstKey }))
                .then(data => cb(null, data))
                .catch(err => cb(err));
        })
        .defer(function(cb) {
            s3Client.send(new GetObjectCommand({ Bucket: 'mapbox', Key: secondKey }))
                .then(data => cb(null, data))
                .catch(err => cb(err));
        })
        .awaitAll(function(err, results) {
            assert.ifError(err, 'all requests completed');
            if (err) return assert.end();

            assert.equal(results[0].count + results[1].count, 1000, 'reported 1000 records');

            var s3results = results.slice(2);
            zlib.gunzip(s3results[0].Body, function(err, first) {
                assert.ifError(err, 'gzipped backup');
                zlib.gunzip(s3results[1].Body, function(err, second) {
                    assert.ifError(err, 'gzipped backup');
                    first = first.toString().trim().split('\n');
                    second = second.toString().trim().split('\n');
                    assert.equal(first.length + second.length, 1000, 'backed up all records');
                    assert.end();
                });
            });
        });
});

dynamodb.delete();
dynamodb.close();
