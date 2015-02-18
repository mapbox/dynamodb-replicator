var test = require('tap').test;
var queue = require('queue-async');
var Dynalite = require('dynalite');
var dynalite;



var config = module.exports.config = {};

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

var dynos = module.exports.dynos = {};

dynos.primary = require('dyno')(config.primary);
dynos.replica = require('dyno')(config.replica);

module.exports.setup = function(t) {
    dynalite = Dynalite({
        createTableMs: 0,
        updateTableMs: 0,
        deleteTableMs: 0
    });
    dynalite.listen(4567, function() {
        t.pass('dynalite listening');

        var q = queue(1);
        q.defer(dynos.primary.createTable, table('test-primary'));
        q.defer(dynos.replica.createTable, table('test-replica'));
        primaryItems.forEach(function(i){
            q.defer(dynos.primary.putItem, i);
        });
        replicaItems.forEach(function(i){
            q.defer(dynos.replica.putItem, i);
        });

        q.awaitAll(function(err, resp) {
            t.notOk(err, 'no error creating tables');
            t.end();
        });
    });
};

module.exports.teardown = function(t) {
    dynalite.close();
    t.end();
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
            "ReadCapacityUnits": 1,
            "WriteCapacityUnits": 1
        },
        "TableName": tableName
    };
};

var primaryItems = [
    {hash: 'hash1', range: 'range1', other:1},
    {hash: 'hash1', range: 'range2', other:2}
];
var replicaItems = [
    {hash: 'hash1', range: 'range3', other:3},
    {hash: 'hash1', range: 'range2', other:10000}
];
