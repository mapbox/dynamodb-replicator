#!/usr/bin/env node

var fs = require('fs');
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
    console.error('  --metric     cloudwatch metric given as namespace/metric-name');
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

backup(config, function(err, details) {
    if (err) log.error(err);

    if (args.metric) {
        var cw = new AWS.CloudWatch({ region: 'us-east-1' });
        var params = {
            Namespace: args.metric.split('/')[0],
            MetricData: []
        };

        if (err) {
            params.MetricData.push({
                MetricName: args.metric.split('/')[1],
                Dimensions: [
                    {
                        Name: 'Type',
                        Value: 'Error'
                    }
                ],
                Value: 1
            });
        }

        if (details) {
            params.MetricData.push({
                MetricName: args.metric.split('/')[1],
                Dimensions: [
                    {
                        Name: 'Type',
                        Value: 'Size'
                    }
                ],
                Value: details.size,
                Unit: 'Bytes'
            }, {
                MetricName: args.metric.split('/')[1],
                Dimensions: [
                    {
                        Name: 'Type',
                        Value: 'RecordCount'
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
