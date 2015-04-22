#!/usr/bin/env node

var Dyno = require('dyno');
var args = require('minimist')(process.argv.slice(2));
var assert = require('assert');

var table = args._[0];
if (!table) {
    console.error('You must specify the name of the table');
    process.exit(1);
}

var key = args._[1];
if (!key) {
    console.error('You must specify the key for the record to check');
    process.exit(1);
}

try { key = JSON.parse(key); }
catch (err) {
    console.error('The key provided is not a valid JSON string');
    process.exit(1);
}

var primary = Dyno({
    table: table,
    region: args.primary || 'us-east-1'
});

var replica = Dyno({
    table: table,
    region: args.replica || 'eu-west-1'
});

primary.getItem(key, function(err, primaryRecord) {
    if (err) throw err;

    replica.getItem(key, function(err, replicaRecord) {
        if (err) throw err;

        console.log('--- Primary record ---');
        console.log(primaryRecord);

        console.log('--- Replica record ---');
        console.log(replicaRecord);

        assert.deepEqual(replicaRecord, primaryRecord, '--- The records are not equivalent ---');
        console.log('--- The records are equivalent ---');
    });
});
