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

    function Aggregate() {
        var aggregation = new stream.Transform({ objectMode: true });
        aggregation.records = [];

        aggregation._transform = function(record, enc, callback) {
            if (aggregation.records.length < 25) {
                aggregation.records.push(record);
                return callback();
            }

            aggregation.push(aggregation.records);
            aggregation.records = [];
            callback();
        };

        aggregation._flush = function(callback) {
            if (aggregation.records.length) aggregation.push(aggregation.records);
            callback();
        };

        return aggregation;
    }

    function Compare(read, keySchema, deleteMissing) {
        var noItem = deleteMissing ? 'extraneous' : 'missing';
        var comparison = new stream.Transform({ objectMode: true });
        comparison.discrepancies = 0;

        comparison._transform = function(records, enc, callback) {
            var recordKeys = records.reduce(function(recordKeys, record) {
                recordKeys.push(keySchema.reduce(function(key, attribute) {
                    key[attribute] = record[attribute];
                    return key;
                }, {}));
                return recordKeys;
            }, []);

            var indexedRecords = records.reduce(function(indexedRecords, record, i) {
                indexedRecords[JSON.stringify(recordKeys[i])] = record;
                return indexedRecords;
            }, {});

            if (config.backfill) {
                Object.keys(indexedRecords).forEach(function(key) {
                    var record = indexedRecords[key];
                    log('[backfill] %s', key);
                    comparison.discrepancies++;
                    itemsCompared++;
                    comparison.push({ put: record });
                });

                return callback();
            }

            read.getItems(recordKeys, function(err, items) {
                if (err) return callback(err);

                var itemKeys = items.reduce(function(itemKeys, item) {
                    itemKeys.push(keySchema.reduce(function(key, attribute) {
                        key[attribute] = item[attribute];
                        return key;
                    }, {}));
                    return itemKeys;
                }, []);

                var indexedItems = items.reduce(function(indexedItems, item, i) {
                    indexedItems[JSON.stringify(itemKeys[i])] = item;
                    return indexedItems;
                }, {});

                recordKeys.forEach(function(key) {
                    var item = indexedItems[JSON.stringify(key)];

                    if (!item) {
                        itemsCompared++;
                        comparison.discrepancies++;
                        var record = indexedRecords[JSON.stringify(key)];
                        log('[%s] %j', noItem, key);
                        if (!config.repair) return;
                        if (deleteMissing) comparison.push({ remove: key });
                        else comparison.push({ put: record });
                    }
                });

                _(indexedItems).each(function(item, key) {
                    itemsCompared++;
                    var record = indexedRecords[key];

                    try { assert.deepEqual(record, item); }
                    catch (notEqual) {
                        comparison.discrepancies++;
                        log('[different] %s', key);
                        if (!config.repair) return;
                        comparison.push({ put: record });
                    }
                });

                callback();
            });
        };

        return comparison;
    }

    function Write() {
        var writer = new stream.Writable({ objectMode: true, highWaterMark: 40 });
        writer.puts = [];
        writer.deletes = [];

        writer._write = function(item, enc, callback) {
            if (!item.put && !item.remove)
                return callback(new Error('Invalid item sent to writer: %j', item));

            var buffer = item.put ? writer.puts : writer.deletes;
            if (buffer.length < 25) {
                buffer.push(item.put || item.remove);
                return callback();
            }

            if (item.put) {
                return replica.putItems(writer.puts, function(err) {
                    if (err) return callback(err);
                    writer.puts = [item.put];
                    callback();
                });
            }

            if (item.remove) {
                return replica.deleteItems(writer.deletes, function(err) {
                    if (err) return callback(err);
                    writer.deletes = [item.remove];
                    callback();
                });
            }
        };

        var streamEnd = writer.end.bind(writer);
        writer.end = function() {
            var q = queue();

            if (writer.puts.length) q.defer(replica.putItems, writer.puts);
            if (writer.deletes.length) q.defer(replica.deleteItems, writer.deletes);

            q.awaitAll(function(err) {
                if (err) return writer.emit('error', err);
                streamEnd();
            });
        };

        return writer;
    }

    primary.describeTable(function(err, description) {
        if (err) return done(err);
        var keySchema = _(description.Table.KeySchema).pluck('AttributeName');
        scanPrimary(keySchema);
    });

    function scanPrimary(keySchema) {
        var aggregate = Aggregate();
        var compare = Compare(replica, keySchema, false);
        var write = Write();

        log('Scanning primary table and comparing to replica');

        primary.scan(scanOpts)
            .on('dbrequest', progress)
            .on('error', finish)
          .pipe(aggregate)
            .on('error', finish)
          .pipe(compare)
            .on('error', finish)
          .pipe(write)
            .on('error', finish)
            .on('finish', function() {
                discrepancies += compare.discrepancies;
                log('[discrepancies] %s', compare.discrepancies);
                if (!config.backfill) return scanReplica(keySchema);
                finish();
            });
    }

    function scanReplica(keySchema) {
        var aggregate = Aggregate();
        var compare = Compare(primary, keySchema, true);
        var write = Write();

        log('Scanning replica table and comparing to primary');

        replica.scan(scanOpts)
            .on('dbrequest', progress)
            .on('error', finish)
          .pipe(aggregate)
            .on('error', finish)
          .pipe(compare)
            .on('error', finish)
          .pipe(write)
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
