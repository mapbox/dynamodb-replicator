#!/usr/bin/env node

var fs = require('fs');
var diff = require('../diff');
var split = require('split');
var queue = require('queue-async');
var fastlog = require('fastlog');

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
config.log = fastlog('diff-tables', 'info');

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

diff(config, function(err, discrepancies) {
    if (err) {
        config.log.error(err);
        process.exit(1);
    }
});
