var AWS = require('aws-sdk');
var Dyno = require('dyno');
var stream = require('stream');
var zlib = require('zlib');

module.exports = function(config, done) {
    var primary = Dyno(config);
    var s3 = new AWS.S3();

    var log = config.log || console.log;
    var scanOpts = config.hasOwnProperty('segment') && config.segments ?
        { Segment: config.segment, TotalSegments: config.segments } : undefined;

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

    var data = primary.scanStream(scanOpts)
        .on('error', next)
      .pipe(stringify)
        .on('error', next)
      .pipe(zlib.createGzip());

    log('[segment %s] Starting backup job %s of %s', index, config.backup.jobid, config.region + '/' + config.table);

    s3.upload({
        ServerSideEncryption: process.env.ServerSideEncryption || 'AES256',
        SSEKMSKeyId: process.env.SSEKMSKeyId,
        Bucket: config.backup.bucket,
        Key: key,
        Body: data
    }, function(err) {
        if (err) return next(err);
        log('[segment %s] Uploaded dynamo backup to s3://%s/%s', index, config.backup.bucket, key);
        log('[segment %s] Wrote %s items to backup', index, count);
        next();
    }).on('httpUploadProgress', function(progress) {
        log('[segment %s] Uploaded %s bytes', index, progress.loaded);
        size = progress.total;
    });

    function next(err) {
        if (err) return done(err);
        done(null, { size: size, count: count });
    }
};
