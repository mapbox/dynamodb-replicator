var _ = require('underscore');
var queue = require('queue-async');
var Dyno = require('dyno');
var stream = require('stream');
var assert = require('assert');

module.exports = function(config, done) {
    var primary = Dyno(config.primary);
    var replica = Dyno(config.replica);
    primary.tableName = config.primary.table;
    replica.tableName = config.replica.table;
    primary.name = 'primary';
    replica.name = 'replica';

    var log = config.log || console.log;
    var scanOpts = config.hasOwnProperty('segment') && config.segments ?
        { Segment: config.segment, TotalSegments: config.segments } : undefined;

    var discrepancies = 0;
    var itemsScanned = 0;
    var itemsCompared = 0;
    var start = Date.now();

    function report() {
        var elapsed = (Date.now() - start) / 1000;
        var scanRate = Math.min(itemsScanned, (itemsScanned / elapsed).toFixed(2));
        var compareRate = Math.min(itemsCompared, (itemsCompared / elapsed).toFixed(2));
        log('[progress] Scan rate: %s items @ %s items/s | Compare rate: %s items/s', itemsScanned, scanRate, compareRate);
    }

    var reporter = setInterval(report, 60000).unref();

    function Aggregate() {
        var aggregation = new stream.Transform({ objectMode: true });
        aggregation.records = [];

        aggregation._transform = function(record, enc, callback) {
            aggregation.records.push(record);

            if (aggregation.records.length < 25) return callback();

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

    function Compare(readFrom, compareTo, keySchema, deleteMissing) {
        var noItem = deleteMissing ? 'extraneous' : 'missing';
        var comparison = new stream.Transform({ objectMode: true });
        comparison.discrepancies = 0;

        comparison._transform = function(records, enc, callback) {
            var params = { RequestItems: {} };
            params.RequestItems[readFrom.tableName] = { Keys: [] };
            itemsScanned += records.length;

            var recordKeys = records.reduce(function(recordKeys, record) {
                var key = keySchema.reduce(function(key, attribute) {
                    key[attribute] = record[attribute];
                    return key;
                }, {});
                params.RequestItems[readFrom.tableName].Keys.push(key);
                recordKeys.push(key);
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

            var items = [];
            (function read(params) {
                readFrom.batchGetItem(params, function(err, data) {
                    if (err) return callback(err);

                    items = items.concat(data.Responses[readFrom.tableName]);

                    if (Object.keys(data.UnprocessedKeys).length)
                        return read({ RequestItems: data.UnprocessedKeys });

                    gotAll();
                })
            })(params);

            function gotAll() {
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

                var q = queue();

                // Find missing records -- scan gave us a key but the batch read did not find a match
                recordKeys.forEach(function(key) {
                    var item = indexedItems[JSON.stringify(key)];

                    if (!item) {
                        q.defer(function(next) {
                            compareTo.getItem({ Key: key, ConsistentRead: true }, function(err, data) {
                                itemsCompared++;
                                if (err) return next(err);
                                var record = data.Item;

                                if (record) {
                                    comparison.discrepancies++;
                                    log('[%s] %j', noItem, key);
                                    if (!config.repair) return next();
                                    if (deleteMissing) comparison.push({ remove: key });
                                    else comparison.push({ put: record });
                                }

                                next();
                            });
                        });
                    }
                });

                // Find differing records -- iterate through each item that we did find in the batch read
                _(indexedItems).each(function(item, key) {
                    itemsCompared++;
                    var record = indexedRecords[key];
                    var recordString = Dyno.serialize(record);
                    var itemString = Dyno.serialize(item);

                    try { assert.deepEqual(JSON.parse(recordString), JSON.parse(itemString)); }
                    catch (notEqual) {
                        q.defer(function(next) {
                            compareTo.getItem({ Key: JSON.parse(key), ConsistentRead: true }, function(err, data) {
                                if (err) return next(err);
                                var record = data.Item;

                                var recordString = Dyno.serialize(record);

                                try { assert.deepEqual(JSON.parse(recordString), JSON.parse(itemString)); }
                                catch (notEqual) {
                                    comparison.discrepancies++;
                                    log('[different] %s', key);
                                    if (!config.repair) return next();
                                    comparison.push({ put: record });
                                }

                                next();
                            });
                        });
                    }
                });

                q.awaitAll(function(err) { callback(err); });
            }
        };

        return comparison;
    }

    function Write() {
        var writer = new stream.Writable({ objectMode: true, highWaterMark: 40 });
        writer.params = { RequestItems: {} };
        writer.params.RequestItems[replica.tableName] = [];
        writer.pending = false;

        writer._write = function(item, enc, callback) {
            if (!item.put && !item.remove) {
                return callback(new Error('Invalid item sent to writer: ' + JSON.stringify(item)));
            }

            var buffer = writer.params.RequestItems[replica.tableName];
            buffer.push(item.put ? { PutRequest: { Item: item.put } } : { DeleteRequest: { Key: item.remove } });
            if (buffer.length < 25) return callback();

            (function write(params) {
                writer.pending = true;
                replica.batchWriteItem(params, function(err, data) {
                    writer.pending = false;
                    if (err) return callback(err);

                    if (data.UnprocessedKeys && Object.keys(data.UnprocessedKeys).length)
                        return write({ RequestItems: data.UnprocessedKeys });

                    writer.params.RequestItems[replica.tableName] = [];
                    callback();
                });
            })(writer.params);
        };

        var streamEnd = writer.end.bind(writer);
        writer.end = function() {
            if (writer.pending) return setImmediate(writer.end);

            if (!writer.params.RequestItems[replica.tableName].length)
                return streamEnd();

            (function write(params) {
                replica.batchWriteItem(params, function(err, data) {
                    if (err) return streamEnd(err);

                    if (data.UnprocessedKeys && Object.keys(data.UnprocessedKeys).length)
                        return write({ RequestItems: data.UnprocessedKeys });

                    streamEnd();
                });
            })(writer.params);
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
        var compare = Compare(replica, primary, keySchema, false);
        var write = Write();

        log('Scanning primary table and comparing to replica');

        primary.scanStream(scanOpts)
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
        var compare = Compare(primary, replica, keySchema, true);
        var write = Write();

        log('Scanning replica table and comparing to primary');

        replica.scanStream(scanOpts)
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
