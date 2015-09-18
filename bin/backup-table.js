#!/usr/bin/env node

var backup = require('../backup');
var fastlog = require('fastlog');
var args = require('minimist')(process.argv.slice(2));
var crypto = require('crypto');
var s3urls = require('s3urls');
var AWS = require('aws-sdk');

function usage() {
    console.error('');
    console.error('Usage: backup-table region/table s3url');
    console.error('');
    console.error('Options:');
    console.error('  --jobid      assign a jobid to this backup');
    console.error('  --segment    segment identifier (0-based)');
    console.error('  --segments   total number of segments');
    console.error('  --metric     cloudwatch metric namespace. Will provide dimension TableName = the name of the backed-up table.');
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

backup(config, function(err, details) {
    if (err) log.error(err);

    if (args.metric) {
        var cw = new AWS.CloudWatch({ region: config.region });
        var params = {
            Namespace: args.metric,
            MetricData: []
        };

        if (err) {
            params.MetricData.push({
                MetricName: 'BackupErrors',
                Dimensions: [
                    {
                        Name: 'TableName',
                        Value: config.table
                    }
                ],
                Value: 1
            });
        }

        if (details) {
            params.MetricData.push({
                MetricName: 'BackupSize',
                Dimensions: [
                    {
                        Name: 'TableName',
                        Value: config.table
                    }
                ],
                Value: details.size,
                Unit: 'Bytes'
            }, {
                MetricName: 'BackupRecordCount',
                Dimensions: [
                    {
                        Name: 'TableName',
                        Value: config.table
                    }
                ],
                Value: details.count,
                Unit: 'Count'
            });
        }

        cw.putMetricData(params, function(err) {
            if (err) log.error(err);
        });
    }
});
