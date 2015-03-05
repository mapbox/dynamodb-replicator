var _ = require('underscore');
var queue = require('queue-async');
var Dyno = require('dyno');
var stream = require('stream');

module.exports = function(config, done) {
    var primary = Dyno(config.primary);
    var replica = Dyno(config.replica);
    primary.name = 'primary';
    replica.name = 'replica';

    var log = config.log || console.log;
    var scanOpts = config.hasOwnProperty('segment') && config.segments ?
        { segment: config.segment, segments: config.segments } : undefined;

    var discrepancies = 0;

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
                    log('[%s] %j', noItem, key);
                    if (!config.repair) return callback();
                    if (deleteMissing) return replica.deleteItem(key, callback);
                    return replica.putItem(record, callback);
                }

                if (!_.isEqual(record, item)) {
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
            .on('error', done)
            .pipe(compare)
            .on('error', done)
            .on('finish', function() {
                discrepancies += compare.discrepancies;
                log('[discrepancies] %s', compare.discrepancies);
                if (!config.backfill) return scanReplica(keySchema);
                done(null, discrepancies);
            });
    }

    function scanReplica(keySchema) {
        var compare = Compare(primary, keySchema, true);

        log('Scanning replica table and comparing to primary');

        replica.scan(scanOpts)
            .on('error', done)
            .pipe(compare)
            .on('error', done)
            .on('finish', function() {
                discrepancies += compare.discrepancies;
                log('[discrepancies] %s', compare.discrepancies);
                done(null, discrepancies);
            });
    }
};
