var { S3Client } = require('@aws-sdk/client-s3');
var { Upload } = require('@aws-sdk/lib-storage');
var s3scan = require('@mapbox/s3scan');
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
        region: config.region || 'us-east-1',
        requestHandler: {
            connectionTimeout: 1000,
            httpAgent: new AgentKeepAlive.HttpsAgent({
                keepAlive: true,
                maxSockets: 256,
                keepAliveMsecs: 60000
            })
        }
    };
    if (config.maxRetries) s3Options.maxAttempts = config.maxRetries;
    if (config.logger) s3Options.logger = config.logger;

    var s3Client = new S3Client(s3Options);

    var size = 0;
    var uri = ['s3:/', config.source.bucket, config.source.prefix].join('/');
    var partsLoaded = -1;

    var objStream = s3scan.Scan(uri, { s3: s3Client })
        .on('error', function(err) { done(err); });
    var gzip = zlib.createGzip()
        .on('error', function(err) { done(err); });

    var stringify = new stream.Transform();
    stringify._writableState.objectMode = true;
    stringify._transform = function(data, enc, callback) {
        if (!data || !data.Body ) return callback();
        callback(null, data.Body.toString() + '\n');
    };

    log(
        'Starting snapshot from s3://%s/%s to s3://%s/%s',
        config.source.bucket, config.source.prefix,
        config.destination.bucket, config.destination.key
    );

    objStream.pipe(stringify).pipe(gzip);

    const upload = new Upload({
        client: s3Client,
        params: {
            Bucket: config.destination.bucket,
            Key: config.destination.key,
            Body: gzip
        }
    });

    upload.on('httpUploadProgress', function(details) {
        if (details.part !== partsLoaded) {
            log(
                'Starting upload of part #%s, %s bytes uploaded, %s items uploaded @ %s items/s',
                details.part - 1, size,
                objStream.got, objStream.rate()
            );
        }

        partsLoaded = details.part;
        size = details.loaded;
    });

    upload.done()
        .then(() => {
            log('Uploaded snapshot to s3://%s/%s', config.destination.bucket, config.destination.key);
            log('Wrote %s items and %s bytes to snapshot', objStream.got, size);
            done(null, { size: size, count: objStream.got });
        })
        .catch(err => {
            done(err);
        });
};
