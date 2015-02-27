#!/usr/bin/env node

var fs = require('fs');
var diff = require('../diff');
var split = require('split');
var queue = require('queue-async');

var config = {
    primary: {
        region: process.env.PrimaryRegion,
        table: process.env.PrimaryTable
    },
    replica: {
        region: process.env.ReplicaRegion,
        table: process.env.ReplicaTable
    }
};

var args = require('minimist')(process.argv.slice(2));

config.repair = !!args.repair;
config.parallel = Number(args.parallel);

if (args.primary) {
    args.primary = args.primary.split('/');
    config.primary.region = args.primary[0];
    config.primary.table = args.primary[1];
}

if (args.replica) {
    args.replica = args.replica.split('/');
    config.replica.region = args.replica[0];
    config.replica.table = args.replica[1];
}

diff(config, function(err, results) {
    if (err) {
        console.error(err);
        process.exit(1);
    }

    console.log('Errors:');
    console.log(fs.readFileSync(results.errors, 'utf8'));
    console.log('Missing records:');
    console.log(fs.readFileSync(results.missing, 'utf8'));
    console.log('Different records:');
    console.log(fs.readFileSync(results.different, 'utf8'));

    var summary = 'Found ' + results.discrepancies + ' discrepancies';
    if (args.repair) summary = summary + ' and repaired them';
    console.log(summary);
});
