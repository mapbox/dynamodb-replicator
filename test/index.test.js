var test = require('tape');
var tableDef = require('./fixtures/table');
var records = require('./fixtures/records');
var primaryRecords = records(10);
var DynamoDB = require('dynamodb-test');
var primary = DynamoDB(test, 'mapbox-replicator', tableDef);
var replica = DynamoDB(test, 'mapbox-replicator', tableDef);
var stream = require('kinesis-test')(test, 'mapbox-replicator', 1);
var Dyno = require('dyno');
var queue = require('queue-async');
var crypto = require('crypto');

var replicate = require('..');

primary.start();
replica.start();
stream.start();

var dyno = Dyno({
    table: primary.tableName,
    region: 'mock',
    accessKeyId: 'mock',
    secretAccessKey: 'mock',
    endpoint: 'http://localhost:4567',
    kinesisConfig: {
        stream: stream.streamName,
        region: 'mock',
        key: ['id'],
        accessKeyId: 'mock',
        secretAccessKey: 'mock',
        endpoint: 'http://localhost:7654'
    }
});

test('[lambda function] load data', function(assert) {
    dyno.putItems(primaryRecords, function(err) {
        if (err) throw err;
        assert.end();
    });
});

test('[lambda function] make an update', function(assert) {
    primaryRecords[4].data = crypto.randomBytes(256).toString('base64');
    var update = primaryRecords[4];

    dyno.updateItem({ id: update.id }, { put: { data: update.data } }, function(err) {
        if (err) throw err;
        assert.end();
    });
});

test('[lambda function] make a delete', function(assert) {
    var remove = primaryRecords.pop();

    dyno.deleteItem({ id: remove.id }, function(err) {
        if (err) throw err;
        assert.end();
    });
});

test('[lambda function] run', function(assert) {
    process.env.PrimaryTable = primary.tableName;
    process.env.PrimaryRegion = 'mock';
    process.env.PrimaryEndpoint = 'http://localhost:4567';
    process.env.ReplicaTable = replica.tableName;
    process.env.ReplicaRegion = 'mock';
    process.env.ReplicaEndpoint = 'http://localhost:4567';

    var readable = stream.shards[0];
    var records = [];
    readable
        .on('data', function(kinesisRecords) {
            kinesisRecords.forEach(function(record) {
                if (record) records.push({
                    data: record.Data.toString()
                });
            });

            if (records.length === primaryRecords.length) readable.close();
        })
        .on('end', function() {
            replicate(records, checkResults);
        });

    function checkResults(err) {
        assert.ifError(err, 'replicated');

        var q = queue();
        primaryRecords.forEach(function(record) {
            q.defer(function(next) {
                replica.dyno.getItem({ id: record.id }, function(err, item) {
                    if (err) return next(err);
                    assert.deepEqual(record, item, 'expected record in replica');
                    next();
                });
            });
        });

        q.await(function(err) {
            assert.ifError(err, 'found records in replica');
            assert.end();
        });
    }
});

stream.close();
primary.close();
