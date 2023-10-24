#!/usr/bin/env node

var AWS = require('aws-sdk');
var args = require('minimist')(process.argv.slice(2));
var s3urls = require('s3urls');
var fastlog = require('../fastlog');
var snapshot = require('../s3-snapshot');
var fs = require('fs');
var path = require('path');

function usage() {
    console.error('');
    console.error('Usage: incremental-snapshot <source> <dest>');
    console.error('');
    console.error('Options:');
    console.error('  --logger     filename where detailed log messages should be sent');
    console.error('  --retries    number of times that failed S3 requests should be retried');
    console.error('  --metric     cloudwatch metric region/namespace/tablename. Will provide dimension TableName = the tablename.');
}

if (args.help) {
    usage();
    process.exit(0);
}

var source = s3urls.fromUrl(args._[0]);
var dest = s3urls.fromUrl(args._[1]);

if (!source || !dest) {
    console.error('Must provide source and destination S3 locations');
    usage();
    process.exit(1);
}

var log = fastlog('incremental-snapshot', 'info');

var config = {
    log: log.info,
    source: {
        bucket: source.Bucket,
        prefix: source.Key
    },
    destination: {
        bucket: dest.Bucket,
        key: dest.Key
    }
};

if (args.logger)
    config.logger = fs.createWriteStream(path.resolve(args.logger), { flags: 'a' });
if (args.retries)
    config.maxRetries = args.retries;

snapshot(config, function(err, details) {
    if (err) log.error(err);

    if (args.metric) {
        var region = args.metric.split('/')[0];
        var namespace = args.metric.split('/')[1];
        var table = args.metric.split('/')[2];

        var cw = new AWS.CloudWatch({ region: region });

        var params = {
            Namespace: namespace,
            MetricData: []
        };

        if (err) {
            params.MetricData.push({
                MetricName: 'BackupErrors',
                Dimensions: [
                    {
                        Name: 'TableName',
                        Value: table
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
                        Value: table
                    }
                ],
                Value: details.size,
                Unit: 'Bytes'
            }, {
                MetricName: 'BackupRecordCount',
                Dimensions: [
                    {
                        Name: 'TableName',
                        Value: table
                    }
                ],
                Value: details.count,
                Unit: 'Count'
            });
        }

        cw.putMetricData(params, function(err) {
            if (err) return log.error(err);
            if (!details) return log.info('Snapshot failed, wrote error metric to %s', args.metric);
            log.info('Wrote %s size / %s count metrics to %s', details.size, details.count, args.metric);
        });
    }
});
