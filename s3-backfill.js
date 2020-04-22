var Dyno = require('dyno');
var AWS = require('aws-sdk');
var stream = require('stream');
var queue = require('queue-async');
var crypto = require('crypto');
var https = require('https');

module.exports = backfill;

module.exports.agent = new https.Agent({
    keepAlive: true,
    maxSockets: Math.ceil(require('os').cpus().length * 16),
    keepAliveMsecs: 60000
});

function backfill(config, done) {
    var s3 = new AWS.S3({
        maxRetries: 1000,
        httpOptions: {
            timeout: 1000,
            agent: module.exports.agent
        }
    });

    var primary = Dyno(config);

    if (config.backup)
        if (!config.backup.bucket || !config.backup.prefix)
            return done(new Error('Must provide a bucket and prefix for incremental backups'));

    primary.describeTable(function(err, data) {
        if (err) return done(err);

        var keys = data.Table.KeySchema.map(function(schema) {
            return schema.AttributeName;
        });

        var count = 0;
        var starttime = Date.now();

        var writer = new stream.Writable({ objectMode: true, highWaterMark: 1000 });

        writer.queue = queue();
        writer.queue.awaitAll(function(err) { if (err) done(err); });
        writer.pending = 0;

        writer._write = function(record, enc, callback) {
            if (writer.pending > 1000)
                return setImmediate(writer._write.bind(writer), record, enc, callback);

            var key = keys.reduce(function(key, k) {
                key[k] = record[k];
                return key;
            }, {});

            var id = crypto.createHash('md5')
                .update(Dyno.serialize(key))
                .digest('hex');

            var params = {
                ServerSideEncryption: process.env.ServerSideEncryption || 'AES256',
                SSEKMSKeyId: process.env.SSEKMSKeyId,
                Bucket: config.backup.bucket,
                Key: [config.backup.prefix, config.table, id].join('/'),
                Body: Dyno.serialize(record)
            };

            writer.drained = false;
            writer.pending++;
            writer.queue.defer(function(next) {
                s3.putObject(params, function(err) {
                    count++;
                    process.stdout.write('\r\033[K' + count + ' - ' + (count / ((Date.now() - starttime) / 1000)).toFixed(2) + '/s');
                    writer.pending--;
                    if (err) writer.emit('error', err);
                    next();
                });
            });
            callback();
        };

        writer.once('error', done);

        var end = writer.end.bind(writer);
        writer.end = function() {
            writer.queue.awaitAll(end);
        };

        primary.scanStream()
            .on('error', next)
          .pipe(writer)
            .on('error', next)
            .on('finish', next);

        function next(err) {
            if (err) return done(err);
            done(null, { count: count });
        }
    });
}
