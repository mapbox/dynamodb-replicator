#!/usr/bin/env node

var Dyno = require('@mapbox/dyno');
var args = require('minimist')(process.argv.slice(2));
var assert = require('assert');

function usage() {
    console.error('');
    console.error('Usage: diff-record <primary region/table> <replica region/table> <key>');
}

if (args.help) {
    usage();
    process.exit(0);
}

args.primary = args._[0];
if (!args.primary) {
    console.error('You must specify the primary region/table');
    usage();
    process.exit(1);
}

args.replica = args._[1];
if (!args.replica) {
    console.error('You must specify the replica region/table');
    usage();
    process.exit(1);
}

var key = args._[2];
if (!key) {
    console.error('You must specify the key for the record to check');
    usage();
    process.exit(1);
}

// Converts incoming strings in wire or dyno format into dyno format
try {
    var obj = Dyno.deserialize(key);
    for (var k in obj) if (!obj[k]) throw new Error();
    key = obj;

}
catch (err) { key = JSON.parse(key); }

var primaryConfig = {
    table: args.primary.split('/')[1],
    region: args.primary.split('/')[0]
};

if (primaryConfig.region === 'local') {
    primaryConfig.accessKeyId = 'fake';
    primaryConfig.secretAccessKey = 'fake';
    primaryConfig.endpoint = 'http://localhost:4567';
}

var primary = Dyno(primaryConfig);

var replicaConfig = {
    table: args.replica.split('/')[1],
    region: args.replica.split('/')[0]
};

if (replicaConfig.region === 'local') {
    replicaConfig.accessKeyId = 'fake';
    replicaConfig.secretAccessKey = 'fake';
    replicaConfig.endpoint = 'http://localhost:4567';
}

var replica = Dyno(replicaConfig);

primary.getItem({ Key: key }, function(err, data) {
    if (err) throw err;
    var primaryRecord = data.Item;

    replica.getItem({ Key: key }, function(err, data) {
        if (err) throw err;
        var replicaRecord = data.Item;

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
