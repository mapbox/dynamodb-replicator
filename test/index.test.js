var test = require('tape');
var tableDef = require('./fixtures/table');
var DynamoDB = require('dynamodb-test');
var replica = DynamoDB(test, 'mapbox-replicator', tableDef);
var Dyno = require('dyno');
var path = require('path');
var events = path.resolve(__dirname, 'fixtures', 'events');
var main = require('..');
var replicate = require('..').replicate;
var backup = require('..').backup;
var _ = require('underscore');
var crypto = require('crypto');
var AWS = require('aws-sdk');
var s3 = new AWS.S3();
var queue = require('queue-async');

replica.start();

var dyno = Dyno({
    table: replica.tableName,
    region: 'mock',
    accessKeyId: 'mock',
    secretAccessKey: 'mock',
    endpoint: 'http://localhost:4567'
});

process.env.ReplicaTable = replica.tableName;
process.env.ReplicaRegion = 'mock';
process.env.ReplicaEndpoint = 'http://localhost:4567';
process.env.AWS_ACCESS_KEY_ID = 'mock';
process.env.AWS_SECRET_ACCESS_KEY = 'mock';

var httpsAgent;
test('[agent] use http agent for replication tests', function(assert) {
    httpsAgent = main.agent;
    main.agent = require('http').globalAgent;
    assert.end();
});

replica.test('[replicate] insert', function(assert) {
    var event = require(path.join(events, 'insert.json'));
    replicate(event, {}, function(err) {
        assert.ifError(err, 'success');
        dyno.scan(function(err, data) {
            if (err) throw err;
            assert.deepEqual(data, { Count: 1, Items: [{ id: 'record-1', range: 1 }], ScannedCount: 1 }, 'inserted desired record');
            assert.end();
        });
    });
});

replica.test('[replicate] insert & modify', function(assert) {
    var event = require(path.join(events, 'insert-modify.json'));
    replicate(event, {}, function(err) {
        assert.ifError(err, 'success');
        dyno.scan(function(err, data) {
            if (err) throw err;
            assert.deepEqual(data, { Count: 1, Items: [{ id: 'record-1', range: 2 }], ScannedCount: 1 }, 'inserted & modified desired record');
            assert.end();
        });
    });
});

replica.test('[replicate] insert, modify & delete', function(assert) {
    var event = require(path.join(events, 'insert-modify-delete.json'));
    replicate(event, {}, function(err) {
        assert.ifError(err, 'success');
        dyno.scan(function(err, data) {
            if (err) throw err;
            assert.deepEqual(data, { Count: 0, Items: [], ScannedCount: 0 }, 'inserted, modified, and deleted desired record');
            assert.end();
        });
    });
});

replica.test('[replicate] adjust many', function(assert) {
    var event = require(path.join(events, 'adjust-many.json'));
    replicate(event, {}, function(err) {
        assert.ifError(err, 'success');
        dyno.scan(function(err, data) {
            if (err) throw err;

            var expected = [
                { range: 22, id: 'record-2' },
                { range: 33, id: 'record-3' }
            ];

            data = data.Items.map(Dyno.serialize);
            expected = expected.map(Dyno.serialize);

            assert.equal(
                _.intersection(data, expected).length,
                expected.length,
                'adjusted many records correctly'
            );

            assert.end();
        });
    });
});

replica.test('[lambda] insert with buffers', function(assert) {
    var event = require(path.join(events, 'insert-buffer.json'));
    replicate(event, {}, function(err) {
        assert.ifError(err, 'success');
        dyno.scan(function(err, data) {
            if (err) throw err;

            var expected = {
                range: 1,
                id: 'record-1',
                val: new Buffer('hello'),
                map: { prop: new Buffer('hello') },
                list: ['string', new Buffer('hello')],
                bufferSet: Dyno.createSet([new Buffer('hello')], 'B')
            };

            data = data.Items[0];

            assert.equal(data.range, expected.range, 'expected range');
            assert.equal(data.id, expected.id, 'expected id');
            assert.deepEqual(data.val, expected.val, 'expected val');
            assert.deepEqual(data.map, expected.map, 'expected map');
            assert.deepEqual(data.list, expected.list, 'expected list');
            assert.deepEqual(data.bufferSet.contents, expected.bufferSet.contents, 'expected bufferSet.contents');
            assert.end();
        });
    });
});

test('[agent] return agent to normal', function(assert) {
    main.agent = httpsAgent;
    assert.end();
});

test('[incremental backup] configurable region', function(assert) {
    process.env.BackupRegion = 'fake';
    assert.plan(2);

    var S3 = AWS.S3;
    AWS.S3 = function(config) {
        assert.equal(config.region, 'fake', 'configured region on S3 client');
    };

    backup({ Records: [] }, {}, function(err) {
        assert.ifError(err, 'backup success');
        AWS.S3 = S3;
        delete process.env.BackupRegion;
    });
});

test('[incremental backup] insert', function(assert) {
    process.env.BackupPrefix = 'dynamodb-replicator/test/' + crypto.randomBytes(4).toString('hex');

    var event = require(path.join(events, 'insert.json'));
    var table = event.Records[0].eventSourceARN.split('/')[1];
    var id = crypto.createHash('md5')
        .update(JSON.stringify(event.Records[0].dynamodb.Keys))
        .digest('hex');

    backup(event, {}, function(err) {
        assert.ifError(err, 'success');

        s3.getObject({
            Bucket: process.env.BackupBucket,
            Key: [process.env.BackupPrefix, table, id].join('/')
        }, function(err, data) {
            assert.ifError(err, 'no S3 error');
            assert.ok(data.Body, 'got S3 object');

            var found = JSON.parse(data.Body.toString());
            var expected = { range: { N:'1' }, id: { S: 'record-1' } };
            assert.deepEqual(found, expected, 'expected item put to S3');
            assert.end();
        });
    });
});

test('[incremental backup] insert & modify', function(assert) {
    process.env.BackupPrefix = 'dynamodb-replicator/test/' + crypto.randomBytes(4).toString('hex');

    var event = require(path.join(events, 'insert-modify.json'));
    var table = event.Records[0].eventSourceARN.split('/')[1];
    var id = crypto.createHash('md5')
        .update(JSON.stringify(event.Records[0].dynamodb.Keys))
        .digest('hex');

    backup(event, {}, function(err) {
        assert.ifError(err, 'success');

        s3.getObject({
            Bucket: process.env.BackupBucket,
            Key: [process.env.BackupPrefix, table, id].join('/')
        }, function(err, data) {
            assert.ifError(err, 'no S3 error');
            assert.ok(data.Body, 'got S3 object');

            var found = JSON.parse(data.Body.toString());
            var expected = { range: { N:'2' }, id: { S: 'record-1' } };
            assert.deepEqual(found, expected, 'expected item modified on S3');
            assert.end();
        });
    });
});

test('[incremental backup] insert, modify & delete', function(assert) {
    process.env.BackupPrefix = 'dynamodb-replicator/test/' + crypto.randomBytes(4).toString('hex');

    var event = require(path.join(events, 'insert-modify-delete.json'));
    var table = event.Records[0].eventSourceARN.split('/')[1];
    var id = crypto.createHash('md5')
        .update(JSON.stringify(event.Records[0].dynamodb.Keys))
        .digest('hex');

    backup(event, {}, function(err) {
        assert.ifError(err, 'success');

        s3.getObject({
            Bucket: process.env.BackupBucket,
            Key: [process.env.BackupPrefix, table, id].join('/')
        }, function(err) {
            assert.equal(err.code, 'NoSuchKey', 'object was deleted');
            assert.end();
        });
    });
});

test('[incremental backup] adjust many', function(assert) {
    process.env.BackupPrefix = 'dynamodb-replicator/test/' + crypto.randomBytes(4).toString('hex');

    var event = require(path.join(events, 'adjust-many.json'));
    var table = event.Records[0].eventSourceARN.split('/')[1];

    var expected = [
        { range: { N: '22' }, id: { S: 'record-2' } },
        { range: { N: '33' }, id: { S: 'record-3' } }
    ];

    backup(event, {}, function(err) {
        assert.ifError(err, 'success');
        var q = queue();

        expected.forEach(function(record) {
            q.defer(function(next) {
                var key = { id: record.id };
                var id = crypto.createHash('md5')
                    .update(JSON.stringify(key))
                    .digest('hex');

                s3.getObject({
                    Bucket: process.env.BackupBucket,
                    Key: [process.env.BackupPrefix, table, id].join('/')
                }, function(err, data) {
                    assert.ifError(err, 'no S3 error for ' + JSON.stringify(key));
                    if (!data) return next();
                    assert.ok(data.Body, 'got S3 object for ' + JSON.stringify(key));

                    var found = JSON.parse(data.Body.toString());
                    assert.deepEqual(found, record, 'expected item modified on S3 for ' + JSON.stringify(key));
                    next();
                });
            });
        });

        q.defer(function(next) {
            var id = crypto.createHash('md5')
                .update(JSON.stringify({ id: { S: 'record-1' } }))
                .digest('hex');

            s3.getObject({
                Bucket: process.env.BackupBucket,
                Key: [process.env.BackupPrefix, table, id].join('/')
            }, function(err) {
                assert.equal(err.code, 'NoSuchKey', 'object was deleted');
                next();
            });
        });

        q.awaitAll(function() {
            assert.end();
        });
    });
});

replica.close();
