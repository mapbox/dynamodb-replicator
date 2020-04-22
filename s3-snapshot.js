var AWS = require('aws-sdk');
var s3scan = require('s3scan');
var zlib = require('zlib');
var stream = require('stream');
var AgentKeepAlive = require('agentkeepalive');

module.exports = function(config, done) {
    var log = config.log || console.log;

    if (!config.source || !config.source.bucket || !config.source.prefix)
        return done(new Error('Must provide source bucket and prefix to snapshot'));

    if (!config.destination || !config.destination.bucket || !config.destination.key)
        return done(new Error('Must provide destination bucket and key where the snapshot will be put'));

    var s3Options = {
        httpOptions: {
            timeout: 1000,
            agent: new AgentKeepAlive.HttpsAgent({
                keepAlive: true,
                maxSockets: 256,
                keepAliveTimeout: 60000
            })
        }
    };
    if (config.maxRetries) s3Options.maxRetries = config.maxRetries;
    if (config.logger) s3Options.logger = config.logger;

    var s3 = new AWS.S3(s3Options);

    var size = 0;
    var uri = ['s3:/', config.source.bucket, config.source.prefix].join('/');
    var partsLoaded = -1;

    var objStream = s3scan.Scan(uri, { s3: s3 })
        .on('error', function(err) { done(err); });
    var gzip = zlib.createGzip()
        .on('error', function(err) { done(err); });

    var stringify = new stream.Transform();
    stringify._writableState.objectMode = true;
    stringify._transform = function(data, enc, callback) {
        if (!data) return callback();
        callback(null, data.Body.toString() + '\n');
    };

    var upload = s3.upload({
        ServerSideEncryption: process.env.ServerSideEncryption || 'AES256',
        SSEKMSKeyId: process.env.SSEKMSKeyId,
        Bucket: config.destination.bucket,
        Key: config.destination.key,
        Body: gzip
    }).on('httpUploadProgress', function(details) {
        if (details.part !== partsLoaded) {
            log(
                'Starting upload of part #%s, %s bytes uploaded, %s items uploaded @ %s items/s',
                details.part - 1, size,
                objStream.got, objStream.rate()
            );
        }

        partsLoaded = details.part;
        size = details.loaded;
    }).on('error', function(err) { done(err); });

    log(
        'Starting snapshot from s3://%s/%s to s3://%s/%s',
        config.source.bucket, config.source.prefix,
        config.destination.bucket, config.destination.key
    );

    objStream.pipe(stringify).pipe(gzip);

    upload.send(function(err) {
        if (err) return done(err);

        log('Uploaded snapshot to s3://%s/%s', config.destination.bucket, config.destination.key);
        log('Wrote %s items and %s bytes to snapshot', objStream.got, size);
        done(null, { size: size, count: objStream.got });
    });
};
