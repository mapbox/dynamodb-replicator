var AWS = require('aws-sdk');
var s3Stream = require('s3-upload-stream')(new AWS.S3());
var Dyno = require('dyno');
var stream = require('stream');
var zlib = require('zlib');

module.exports = function(config, done) {
    var primary = Dyno(config);

    var log = config.log || console.log;
    var scanOpts = config.hasOwnProperty('segment') && config.segments ?
        { segment: config.segment, segments: config.segments } : undefined;

    if (config.backup)
        if (!config.backup.bucket || !config.backup.prefix || !config.backup.jobid)
            return done(new Error('Must provide a bucket, prefix and jobid for backups'));

    var index = !isNaN(parseInt(config.segment)) ? config.segment.toString() : 0;
    var key = [config.backup.prefix, config.backup.jobid, index].join('/');
    var count = 0;
    var size = 0;

    var stringify = new stream.Transform({ objectMode: true });
    stringify._transform = function(record, enc, callback) {
        var line = Dyno.serialize(record);

        setImmediate(function() {
            stringify.push(line + '\n');
            count++;
            callback();
        });
    };

    var upload = s3Stream.upload({
        Bucket: config.backup.bucket,
        Key: key
    });

    log('[segment %s] Starting backup job %s of %s', index, config.backup.jobid, config.region + '/' + config.table);
    primary.scan(scanOpts)
        .on('error', next)
      .pipe(stringify)
        .on('error', next)
      .pipe(zlib.createGzip())
      .pipe(upload)
        .on('error', next)
        .on('part', function(details) {
            log('[segment %s] Uploaded part #%s for total %s bytes uploaded', index, details.PartNumber, details.uploadedSize);
            size = details.uploadedSize;
        })
        .on('uploaded', function() {
            log('[segment %s] Uploaded dynamo backup to s3://%s/%s', index, config.backup.bucket, key);
            log('[segment %s] Wrote %s items to backup', index, count);
            next();
        });

    function next(err) {
        if (err) return done(err);
        done(null, { size: size, count: count });
    }
};
