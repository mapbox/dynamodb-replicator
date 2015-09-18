#!/usr/bin/env node

var minimist = require('minimist');
var s3urls = require('s3urls');
var Dyno = require('dyno');
var backup = require('..').backup;

var args = minimist(process.argv.slice(2));

function usage() {
    console.error('');
    console.error('Usage: incremental-backup-record <tableinfo> <s3url> <recordkey>');
    console.error(' - tableinfo: the table to backup from, specified as `region/tablename`');
    console.error(' - s3url: s3 folder into which the record should be backed up to');
    console.error(' - recordkey: the key for the record specified as a JSON object');
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

var region = table.split('/')[0];
table = table.split('/')[1];

var s3url = args._[1];

if (!s3url) {
    console.error('Must provide an s3url');
    usage();
    process.exit(1);
}

s3url = s3urls.fromUrl(s3url);
process.env.BackupBucket = s3url.Bucket;
process.env.BackupPrefix = s3url.Key;

var key = args._[2];

if (!key) {
    console.error('Must provide a record key');
    usage();
    process.exit(1);
}

// Converts incoming strings in wire or dyno format into dyno format
try { key = Dyno.deserialize(key); }
catch (err) { key = JSON.parse(key); }

var dyno = Dyno({
    region: region,
    table: table
});

dyno.getItem(key, function(err, data) {
    if (err) throw err;

    var event = {
        Records: [
            {
                dynamodb: {
                    Keys: JSON.parse(Dyno.serialize(key)),
                    NewImage: data ? JSON.parse(Dyno.serialize(data)) : undefined
                },
                eventSourceARN: '/' + table,
                eventName: data ? 'INSERT' : 'REMOVE'
            }
        ]
    };

    backup(event, function(err) {
        if (err) throw err;
    });
});
