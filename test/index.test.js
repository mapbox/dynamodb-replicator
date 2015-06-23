var test = require('tape');
var tableDef = require('./fixtures/table');
var DynamoDB = require('dynamodb-test');
var replica = DynamoDB(test, 'mapbox-replicator', tableDef);
var Dyno = require('dyno');
var path = require('path');
var events = path.resolve(__dirname, 'fixtures', 'events');
var replicate = require('..');
var _ = require('underscore');

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

replica.test('[lambda] insert', function(assert) {
    var event = require(path.join(events, 'insert.json'));
    replicate(event, function(err) {
        assert.ifError(err, 'success');
        dyno.scan(function(err, data) {
            if (err) throw err;
            assert.deepEqual(data, [{ range: 1, id: 'record-1' }], 'inserted desired record');
            assert.end();
        });
    });
});

replica.test('[lambda] insert & modify', function(assert) {
    var event = require(path.join(events, 'insert-modify.json'));
    replicate(event, function(err) {
        assert.ifError(err, 'success');
        dyno.scan(function(err, data) {
            if (err) throw err;
            assert.deepEqual(data, [{ range: 2, id: 'record-1' }], 'inserted & modified desired record');
            assert.end();
        });
    });
});

replica.test('[lambda] insert, modify & delete', function(assert) {
    var event = require(path.join(events, 'insert-modify-delete.json'));
    replicate(event, function(err) {
        assert.ifError(err, 'success');
        dyno.scan(function(err, data) {
            if (err) throw err;
            assert.deepEqual(data, [], 'inserted, modified, and deleted desired record');
            assert.end();
        });
    });
});

replica.test('[lambda] adjust many', function(assert) {
    var event = require(path.join(events, 'adjust-many.json'));
    replicate(event, function(err) {
        assert.ifError(err, 'success');
        dyno.scan(function(err, data) {
            if (err) throw err;

            var expected = [
                { range: 22, id: 'record-2' },
                { range: 33, id: 'record-3' },
            ];

            data = data.map(Dyno.serialize);
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

replica.close();
