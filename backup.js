var AWS = require('aws-sdk');
var s3Stream = require('s3-upload-stream')(new AWS.S3());
var queue = require('queue-async');
var Dyno = require('dyno');
var stream = require('stream');

module.exports = function(config, done) {
    var primary = Dyno(config);
    var throughput = require('dynamodb-throughput')(config.table, config);

    var log = config.log || console.log;
    var scanOpts = config.hasOwnProperty('segment') && config.segments ?
        { segment: config.segment, segments: config.segments } : undefined;

    if (config.backup)
        if (!config.backup.bucket || !config.backup.prefix || !config.backup.jobid)
            return done(new Error('Must provide a bucket, prefix and jobid for backups'));

    var index = !isNaN(parseInt(config.segment)) ? config.segment.toString() : 0;
    var key = [config.backup.prefix, config.backup.jobid, index].join('/');

    var writeBackup = new stream.Transform({ objectMode: true });

    writeBackup.upload = s3Stream.upload({
        Bucket: config.backup.bucket,
        Key: key
    }).on('error', function(err) {
        writeBackup.emit('error', err);
    }).on('part', function(details) {
        log('[segment %s] Uploaded part #%s for total %s bytes uploaded', index, details.PartNumber, details.uploadedSize);
    });

    writeBackup.count = 0;

    writeBackup._transform = function(record, enc, callback) {
        var line = JSON.stringify(record, function(key, value) {
            var val = this[key];
            if (Buffer.isBuffer(val)) return 'base64:' + val.toString('base64');
            return value;
        });

        var ready = writeBackup.upload.write(line + '\n');
        writeBackup.count++;

        if (ready) return callback();
        writeBackup.upload.once('drain', callback);
    };

    writeBackup._flush = function(callback) {
        writeBackup.upload.on('uploaded', function(uploadInfo) {
            log('[segment %s] Uploaded dynamo backup to s3://%s/%s', index, config.backup.bucket, key);
            log('[segment %s] Wrote %s items to backup', index, writeBackup.count);
            callback();
        });
        writeBackup.upload.end();
    };

    log('[segment %s] Starting backup job %s of %s', index, config.backup.jobid, config.region + '/' + config.table);

    queue(1)
        .defer(throughput.setCapacity, { read: 1000 })
        .defer(function(next) {
            primary.scan(scanOpts)
                .on('error', next)
                .pipe(writeBackup)
                .on('error', next)
                .on('end', next)
                .resume();
        })
        .awaitAll(function(backupErr) {
            throughput.resetCapacity(function(resetErr) {
                if (backupErr) return done(backupErr);
                if (resetErr) return done(resetErr);
                done();
            });
        });
};
