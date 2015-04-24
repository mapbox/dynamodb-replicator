#!/usr/bin/env node

var Dyno = require('dyno');
var args = require('minimist')(process.argv.slice(2));
var assert = require('assert');

var primary = args._[0];
if (!primary) {
    console.error('You must specify the primary region/table');
    process.exit(1);
}

var replica = args._[1];
if (!primary) {
    console.error('You must specify the replica region/table');
    process.exit(1);
}

var key = args._[2];
if (!key) {
    console.error('You must specify the key for the record to check');
    process.exit(1);
}

try { key = JSON.parse(key); }
catch (err) {
    console.error(key);
    console.error('The key provided is not a valid JSON string');
    process.exit(1);
}

var primary = Dyno({
    table: primary.split('/')[1],
    region: primary.split('/')[0]
});

var replica = Dyno({
    table: replica.split('/')[1],
    region: replica.split('/')[0]
});

primary.getItem(key, function(err, primaryRecord) {
    if (err) throw err;

    replica.getItem(key, function(err, replicaRecord) {
        if (err) throw err;

        console.log('Primary record');
        console.log('--------------');
        console.log(primaryRecord);
        console.log('');

        console.log('Replica record');
        console.log('--------------');
        console.log(replicaRecord);
        console.log('');

        try {
            assert.deepEqual(replicaRecord, primaryRecord);
            console.log('----------------------------');
            console.log('✔ The records are equivalent');
            console.log('----------------------------');
        }
        catch (err) {
            console.log('--------------------------------');
            console.log('✘ The records are not equivalent');
            console.log('--------------------------------');
        }
    });
});
