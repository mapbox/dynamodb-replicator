var _ = require('underscore');
var queue = require('queue-async');
var Dyno = require('dyno');
var stream = require('stream');
var assert = require('assert');

module.exports = function(config, done) {
    var primary = Dyno(config.primary);
    var replica = Dyno(config.replica);
    primary.name = 'primary';
    replica.name = 'replica';

    var log = config.log || console.log;
    var scanOpts = config.hasOwnProperty('segment') && config.segments ?
        { segment: config.segment, segments: config.segments } : undefined;

    var discrepancies = 0;
    var scanRequests = 0;
    var itemsScanned = 0;
    var itemsCompared = 0;
    var start = Date.now();

    function progress(params, response) {
        scanRequests++;
        itemsScanned += response.Count;
        if (response.LastEvaluatedKey)
            log('[progress] LastEvaluatedKey: %j', response.LastEvaluatedKey);
    }

    function report() {
        var elapsed = (Date.now() - start) / 1000;
        var scanRate = Math.min(itemsScanned, (itemsScanned / elapsed).toFixed(2));
        var reqRate = Math.min(scanRequests, (scanRequests / elapsed).toFixed(2));
        var compareRate = Math.min(itemsCompared, (itemsCompared / elapsed).toFixed(2));
        log('[progress] Scan rate: %s items @ %s items/s, %s scans/s | Compare rate: %s items/s', itemsScanned, scanRate, reqRate, compareRate);
    }

    var reporter = setInterval(report, 60000).unref();

    function Compare(read, keySchema, deleteMissing) {
        var writable = new stream.Writable({ objectMode: true });
        var noItem = deleteMissing ? 'extraneous' : 'missing';

        writable.discrepancies = 0;

        writable._write = function(record, enc, callback) {
            var key = keySchema.reduce(function(key, attribute) {
                key[attribute] = record[attribute];
                return key;
            }, {});

            if (config.backfill) {
                log('[backfill] %j', key);
                writable.discrepancies++;
                itemsCompared++;
                return replica.putItem(record, callback);
            }

            read.getItem(key, function(err, item) {
                itemsCompared++;
                if (err) return writable.emit('error', err);

                if (!item) {
                    writable.discrepancies++;
                    log('[%s] %j', noItem, key);
                    if (!config.repair) return callback();
                    if (deleteMissing) return replica.deleteItem(key, callback);
                    return replica.putItem(record, callback);
                }

                try { assert.deepEqual(record, item); }
                catch (notEqual) {
                    writable.discrepancies++;
                    log('[different] %j', key);
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

        log('Scanning primary table and comparing to replica');

        primary.scan(scanOpts)
            .on('dbrequest', progress)
            .on('error', finish)
            .pipe(compare)
            .on('error', finish)
            .on('finish', function() {
                discrepancies += compare.discrepancies;
                log('[discrepancies] %s', compare.discrepancies);
                if (!config.backfill) return scanReplica(keySchema);
                finish();
            });
    }

    function scanReplica(keySchema) {
        var compare = Compare(primary, keySchema, true);

        log('Scanning replica table and comparing to primary');

        replica.scan(scanOpts)
            .on('dbrequest', progress)
            .on('error', finish)
            .pipe(compare)
            .on('error', finish)
            .on('finish', function() {
                discrepancies += compare.discrepancies;
                log('[discrepancies] %s', compare.discrepancies);
                finish();
            });
    }

    function finish(err) {
        clearInterval(reporter);
        report();
        done(err, discrepancies);
    }
};
