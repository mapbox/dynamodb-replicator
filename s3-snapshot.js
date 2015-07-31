var concurrency = Math.ceil(require('os').cpus().length * 16);
require('https').globalAgent.maxSockets = concurrency;

var AWS = require('aws-sdk');
var s3 = new AWS.S3();
var s3Stream = require('s3-upload-stream')(s3);
var queue = require('queue-async');
var zlib = require('zlib');
var stream = require('stream');

function AwsError(err, requestParams) {
    Error.captureStackTrace(err, arguments.callee);
    if (requestParams) err.parameters = requestParams;
    return err;
}

module.exports = function(config, done) {
    var log = config.log || console.log;
    var starttime = Date.now();

    if (!config.source || !config.source.bucket || !config.source.prefix)
        return done(new Error('Must provide source bucket and prefix to snapshot'));

    if (!config.destination || !config.destination.bucket || !config.destination.key)
        return done(new Error('Must provide destination bucket and key where the snapshot will be put'));

    var count = 0;
    var size;

    var upload = s3Stream.upload({
        Bucket: config.destination.bucket,
        Key: config.destination.key
    });

    upload
        .on('error', done)
        .on('part', function(details) {
            log(
                'Uploaded part #%s for total %s bytes, %s items uploaded @ %s items/s',
                details.PartNumber, details.uploadedSize,
                count, (count / ((Date.now() - starttime) / 1000)).toFixed(2)
            );
            size = details.uploadedSize;
        })
        .on('uploaded', function() {
            log('Uploaded snapshot to s3://%s/%s', config.destination.bucket, config.destination.key);
            log('Wrote %s items to snapshot in %s sec', count, (Date.now() - starttime) / 1000);
            done(null, { size: size, count: count });
        });

    var gzip = zlib.createGzip();

    var keyStream = new stream.Readable();
    keyStream.cache = [];
    keyStream.readPending = false;

    keyStream._read = function() {
        var keepReading = true;
        while (keyStream.cache.length && keepReading)
            keepReading = keyStream.push(keyStream.cache.shift());

        if (keyStream.cache.length) return;
        if (keyStream.readPending) return;
        if (keyStream.done) return keyStream.push(null);

        keyStream.readPending = true;

        var params = {
            Bucket: config.source.bucket,
            Prefix: config.source.prefix
        };

        if (keyStream.next) params.Marker = keyStream.next;

        s3.listObjects(params, function(err, data) {
            if (err) return keyStream.emit('error', AwsError(err, params));

            var last = data.Contents.slice(-1)[0];
            var more = data.IsTruncated && last;

            data.Contents.forEach(function(item) {
                keyStream.cache.push(item.Key);
            });

            keyStream.readPending = false;

            if (more) keyStream.next = last.Key;
            else keyStream.done = true;
            keyStream._read();
        });
    };

    var getStream = new stream.Transform();
    getStream.pending = 0;
    getStream.queue = queue();
    getStream.queue.awaitAll(function(err) { if (err) done(err); });

    getStream._transform = function(key, enc, callback) {
        if (getStream.pending > 1000)
            return setImmediate(getStream._transform.bind(getStream), key, enc, callback);

        getStream.pending++;
        getStream.queue.defer(function(next) {
            var params = {
                Bucket: config.source.bucket,
                Key: key.toString()
            };

            (function get(attempts) {
                attempts++;

                s3.getObject(params, function(err, data) {
                    if (err && err.statusCode > 499 && attempts < 6)
                        return setTimeout(get, Math.pow(10, attempts), attempts);
                    getStream.pending--;
                    if (err && err.statusCode !== 404) return next(AwsError(err, params));
                    if (err) log(AwsError(err, params));
                    count++;
                    getStream.push(data.Body + '\n');
                    next();
                });
            })(0);
        });

        callback();
    };

    getStream._flush = function(callback) {
        getStream.queue.awaitAll(callback);
    };

    log(
        'Starting snapshot from s3://%s/%s to s3://%s/%s',
        config.source.bucket, config.source.prefix,
        config.destination.bucket, config.destination.key
    );

    keyStream.pipe(getStream).pipe(gzip).pipe(upload);
};
