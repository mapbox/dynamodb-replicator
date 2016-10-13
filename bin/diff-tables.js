#!/usr/bin/env node

var diff = require('../diff');
var fastlog = require('fastlog');
var args = require('minimist')(process.argv.slice(2));
var crypto = require('crypto');
var parse_location = require('../parse-location')

function usage() {
    console.error('');
    console.error('Usage: diff-tables primary-region/primary-table replica-region/replica-table');
    console.error('');
    console.error('Options:');
    console.error('  --repair     perform actions to fix discrepancies in the replica table');
    console.error('  --segment    segment identifier (0-based)');
    console.error('  --segments   total number of segments');
    console.error('  --backfill   only scan primary table and write to replica');
}

if (args.help) {
    usage();
    process.exit(0);
}

var primary = args._[0];
var replica = args._[1];

if (!primary) {
    console.error('Must provide primary table information');
    usage();
    process.exit(1);
}

if (!replica) {
    config.log.error('Must provide replica table information');
    usage();
    process.exit(1);
}

primary = primary.split('/');
replica = replica.split('/');

var jobid = crypto.randomBytes(8).toString('hex');
var format = '[${timestamp}] [${level}] [${category}] [' + jobid + ']';
var log = fastlog('diff-tables', 'info', format);

var locations = parse_location.parse(primary, replica)
primary = locations[0]
replica = locations[1]

var config = {
    primary: primary,
    replica:replica,
    repair: !!args.repair,
    segment: args.segment,
    segments: args.segments,
    backfill: args.backfill,
    log: log.info
};

diff(config, function(err) {
    if (err) {
        log.error(err);
        process.exit(1);
    }
});
