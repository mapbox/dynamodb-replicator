var test = require('tape');
var primary = require('dynamodb-test')(test, 'dynamodb-replicator', require('./table.json'));
var replica = require('dynamodb-test')(test, 'dynamodb-replicator', require('./table.json'));
var exec = require('child_process').exec;
var queue = require('queue-async');
var diffRecord = require('path').resolve(__dirname, '..', 'bin', 'diff-record.js');

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

primary.start();
primary.load(primaryItems);
replica.start();
replica.load(replicaItems);

test('diff-record', function(assert) {
    queue()
        .defer(function(next) {
            var cmd = [
                diffRecord,
                'local/' + primary.tableName,
                'local/' + replica.tableName,
                '\'{"hash":"hash1","range":"range2"}\''
            ].join(' ');
            exec(cmd, function(err, stdout) {
                assert.ifError(err, '[different] does not error');
                assert.ok(/✘/.test(stdout), '[different] reports difference');
                next();
            });
        })
        .defer(function(next) {
            var cmd = [
                diffRecord,
                'local/' + primary.tableName,
                'local/' + replica.tableName,
                '\'{"hash":"hash1","range":"range4"}\''
            ].join(' ');
            exec(cmd, function(err, stdout) {
                assert.ifError(err, '[equivalent] does not error');
                assert.ok(/✔/.test(stdout), '[equivalent] reports equivalence');
                next();
            });
        })
        .awaitAll(function() {
            assert.end();
        });
});

primary.delete();
replica.delete();
primary.close();
