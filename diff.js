var _ = require('underscore');
var Dyno = require('dyno');
var stream = require('stream');
var assert = require('assert');
var fs = require('fs');
var AWS = require('aws-sdk');
var s3Stream = require('s3-upload-stream')(new AWS.S3());

module.exports = function(config, done) {
    var primary = Dyno(config.primary);
    var replica = Dyno(config.replica);
    primary.name = 'primary';
    replica.name = 'replica';

    var log = config.log || console.log;
    var scanOpts = config.hasOwnProperty('segment') && config.segments ?
        { segment: config.segment, segments: config.segments } : undefined;

    if (config.backup && config.segments > 1)
        return done(new Error('Parallel backups are not supported at this time'));

    if (config.backup)
        if (!config.backup.bucket || !config.backup.prefix)
            return done(new Error('Must provide a bucket and prefix for backups'));

    var discrepancies = 0;

    var writeFile = new stream.Transform({ objectMode: true });

    writeFile.start = function() {
        writeFile.upload = s3Stream.upload({
            Bucket: config.backup.bucket,
            Key: [config.backup.prefix, Date.now() + '.json'].join('/')
        }).on('error', function(err) {
            writeFile.emit('error', err);
        }).on('part', function(details) {
            log('[backup] Uploaded %s bytes to part %s', details.uploadedSize, details.PartNumber);
        });
    };

    writeFile._transform = function(record, enc, callback) {
        var ready = writeFile.upload.write(JSON.stringify(record, replacer) + '\n');
        this.push(record);
        if (ready) return callback();
        writeFile.upload.on('drain', callback);
    };

    writeFile._flush = function(callback) {
        writeFile.upload.on('uploaded', function(uploadInfo) {
            log('[backup] Uploaded dynamo backup to ' + config.backup.bucket + '/' + config.backup.prefix);
            callback();
        });
        writeFile.upload.end();
    };

    function replacer(key) {
        var value = this[key];
        if (Buffer.isBuffer(value)) return 'base64:' + value.toString('base64');
        return value;
    }

    function Compare(read, keySchema, deleteMissing) {
        var writable = new stream.Writable({ objectMode: true });
        var noItem = deleteMissing ? 'extraneous' : 'missing';

        writable.discrepancies = 0;

        writable._write = function(record, enc, callback) {
            var key = keySchema.reduce(function(key, attribute) {
                key[attribute] = record[attribute];
                return key;
            }, {});

            read.getItem(key, function(err, item) {
                if (err) return writable.emit('error', err);

                if (!item) {
                    writable.discrepancies++;
                    log('[repair] [%s] %j', noItem, key);
                    if (!config.repair) return callback();
                    if (deleteMissing) return replica.deleteItem(key, callback);
                    return replica.putItem(record, callback);
                }

                try { assert.deepEqual(record, item); }
                catch (notEqual) {
                    writable.discrepancies++;
                    log('[repair] [different] %j', key);
                    if (!config.repair) return callback();
                    return replica.putItem(record, callback);
                }

                callback();
            });
        };

        return writable;
    }

    primary.describeTable(function(err, description) {
        if (err) return done(err);
        var keySchema = _(description.Table.KeySchema).pluck('AttributeName');
        scanPrimary(keySchema);
    });

    function scanPrimary(keySchema) {
        var compare = Compare(replica, keySchema, false);

        log('[repair] Scanning primary table and comparing to replica');

        var pipeline = primary.scan(scanOpts).on('error', done);
        if (config.backup) {
            writeFile.start();
            pipeline = pipeline.pipe(writeFile).on('error', done);
        }

        pipeline
            .pipe(compare)
            .on('error', done)
            .on('finish', function() {
                discrepancies += compare.discrepancies;
                log('[repair] [discrepancies] %s', compare.discrepancies);
                if (!config.backfill) return scanReplica(keySchema);
                done(null, discrepancies);
            });
    }

    function scanReplica(keySchema) {
        var compare = Compare(primary, keySchema, true);

        log('[repair] Scanning replica table and comparing to primary');

        replica.scan(scanOpts)
            .on('error', done)
            .pipe(compare)
            .on('error', done)
            .on('finish', function() {
                discrepancies += compare.discrepancies;
                log('[repair] [discrepancies] %s', compare.discrepancies);
                done(null, discrepancies);
            });
    }
};
