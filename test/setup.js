var test = require('tape');
var queue = require('queue-async');
var crypto = require('crypto');
var Dynalite = require('dynalite');
var dynalite;

module.exports = function(live) {
    live = !!live;

    var setup = {};

    var config = setup.config = {};

    if (live) {
        config.replica = {
            region: 'us-east-1',
            table: 'dynamodb-replicator-test-replica-' + crypto.randomBytes(4).toString('hex')
        };
        config.primary = {
            region: 'us-east-1',
            table: 'dynamodb-replicator-test-primary-' + crypto.randomBytes(4).toString('hex')
        };
    } else {
        config.replica = {
            accessKeyId: 'fake',
            secretAccessKey: 'fake',
            region: 'eu-west-1',
            table: 'test-replica',
            endpoint: 'http://localhost:4567'
        };

        config.primary = {
            accessKeyId: 'fake',
            secretAccessKey: 'fake',
            region: 'us-east-1',
            table: 'test-primary',
            endpoint: 'http://localhost:4567'
        };
    }

    var dynos = setup.dynos = {};

    dynos.primary = require('dyno')(config.primary);
    dynos.replica = require('dyno')(config.replica);

    setup.setup = function(t) {
        if (live) return setupTables();

        dynalite = Dynalite({
            createTableMs: 0,
            updateTableMs: 0,
            deleteTableMs: 0
        });

        dynalite.listen(4567, function() {
            t.pass('dynalite listening');
            setupTables();
        });

        function setupTables() {
            var q = queue(1);
            q.defer(dynos.primary.createTable, table(config.primary.table));
            q.defer(dynos.replica.createTable, table(config.replica.table));
            primaryItems.forEach(function(i) {
                q.defer(dynos.primary.putItem, i);
            });
            replicaItems.forEach(function(i) {
                q.defer(dynos.replica.putItem, i);
            });

            q.awaitAll(function(err, resp) {
                t.notOk(err, 'no error creating tables');
                t.end();
            });
        }
    };

    // Inserts n different items in both tables
    setup.differentItemsPlease = function(n, callback) {
        function generate() {
            var items = [];
            var data;
            itemsize = 1024;

            for (var i = 0; i < n; i++) {
                data = '';
                for (var k = 0; k < itemsize; k++) {
                    data += Math.floor(10 * Math.random()).toString();
                }

                items.push({
                    hash: 'id:' + i.toString(),
                    range: i.toString(),
                    other: data
                });
            }

            return items;
        }

        var q = queue();
        generate().forEach(function(item) {
            q.defer(dynos.primary.putItem, item);
        });
        generate().forEach(function(item) {
            q.defer(dynos.replica.putItem, item);
        });
        q.awaitAll(callback);
    };

    setup.teardown = function(t) {
        if (!live) {
            dynalite.close();
            return t.end();
        }

        queue(1)
            .defer(dynos.primary.deleteTable, config.primary.table)
            .defer(dynos.replica.deleteTable, config.replica.table)
            .awaitAll(function(err) {
                t.end(err);
            });
    };

    return setup;
};

var table = function(tableName) {
    return {
        "AttributeDefinitions": [
            {
                "AttributeName": "hash",
                "AttributeType": "S"
            },
            {
                "AttributeName": "range",
                "AttributeType": "S"
            }
        ],
        "KeySchema": [
            {
                "AttributeName": "hash",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "range",
                "KeyType": "RANGE"
            }
        ],
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 10,
            "WriteCapacityUnits": 10
        },
        "TableName": tableName
    };
};

var primaryItems = [
    {hash: 'hash1', range: 'range1', other:1},
    {hash: 'hash1', range: 'range2', other:2},
    {hash: 'hash1', range: 'range4', other: new Buffer('hello world')}
];
var replicaItems = [
    {hash: 'hash1', range: 'range2', other:10000},
    {hash: 'hash1', range: 'range3', other:3},
    {hash: 'hash1', range: 'range4', other: new Buffer('hello world')}
];
