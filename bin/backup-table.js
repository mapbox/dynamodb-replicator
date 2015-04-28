#!/usr/bin/env node

var fs = require('fs');
var backup = require('../backup');
var fastlog = require('fastlog');
var args = require('minimist')(process.argv.slice(2));
var crypto = require('crypto');
var s3urls = require('s3urls');

function usage() {
    console.error('');
    console.error('Usage: backup-table region/table s3url');
    console.error('');
    console.error('Options:');
    console.error('  --jobid      assign a jobid to this backup');
    console.error('  --segment    segment identifier (0-based)');
    console.error('  --segments   total number of segments');
}

if (args.help) {
    usage();
    process.exit(0);
}

var table = args._[0];

if (!table) {
    console.error('Must provide table information');
    usage();
    process.exit(1);
}

table = table.split('/');

var s3url = args._[1];

if (!s3url) {
    console.error('Must provide an s3url');
    usage();
    process.exit(1);
}

s3url = s3urls.fromUrl(s3url);

var jobid = args.jobid || crypto.randomBytes(8).toString('hex');
var format = '[${timestamp}] [${level}] [${category}] [' + jobid + ']';
var log = fastlog('backup-table', 'info', format);
var index = !isNaN(parseInt(args.segment)) ? args.segment.toString() : 0;

var config = {
    region: table[0],
    table: table[1],
    segment: args.segment,
    segments: args.segments,
    log: log.info,
    backup: {
        bucket: s3url.Bucket,
        prefix: s3url.Key,
        jobid: jobid
    }
};

log.info('[segment %s] Starting backup job %s of %s', index, jobid, args._[0]);
backup(config, function(err) {
    if (err) {
        log.error(err);
        process.exit(1);
    }
});
