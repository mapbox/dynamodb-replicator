var _ = require('underscore');
var queue = require('queue-async');
var Dyno = require('dyno');
var stream = require('stream');

module.exports = function(config, done) {
    var primary = Dyno(config.primary);
    var replica = Dyno(config.replica);
    primary.name = 'primary';
    replica.name = 'replica';

    var log = config.log || function() {};

    var discrepancies = 0;

    function Compare(read, write, keySchema, deleteMissing) {
        var writable = new stream.Writable({ objectMode: true });

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
                    log('[missing in %s] %j', read.name, key);
                    if (!config.repair) return callback();
                    if (deleteMissing) return write.deleteItem(key, callback);
                    return write.putItem(record, callback);
                }

                if (!_.isEqual(record, item)) {
                    writable.discrepancies++;
                    log('[different in %s] %j', read.name, key);
                    if (!config.repair) return callback();
                    return write.putItem(record, callback);
                }

                callback();
            });
        };

        return writable;
    }

    primary.describeTable(function(err, description) {
        if (err) return callback(err);
        var keySchema = _(description.Table.KeySchema).pluck('AttributeName');
        scanPrimary(keySchema);
    });

    function scanPrimary(keySchema) {
        var compare = Compare(replica, replica, keySchema, false);

        log('Scanning primary table and comparing to replica');

        primary.scan()
            .on('error', done)
            .pipe(compare)
            .on('error', done)
            .on('finish', function() {
                discrepancies += compare.discrepancies;
                log('[discrepancies] Scanning primary: %s', compare.discrepancies);
                scanReplica(keySchema);
            });
    }

    function scanReplica(keySchema) {
        var compare = Compare(primary, replica, keySchema, true);

        log('Scanning replica table and comparing to primary');

        replica.scan()
            .on('error', done)
            .pipe(compare)
            .on('error', done)
            .on('finish', function() {
                discrepancies += compare.discrepancies;
                log('[discrepancies] Scanning replica: %s', compare.discrepancies);
                done(null, discrepancies);
            });
    }
};
