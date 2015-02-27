var fs = require('fs');
var _ = require('underscore');
var queue = require('queue-async');
var path = require('path');
var os = require('os');
var crypto = require('crypto');
var AWS = require('aws-sdk');

module.exports = run;

function tmpfile() {
    var filename = path.join(os.tmpdir(), crypto.randomBytes(8).toString('hex'));
    var stream = fs.createWriteStream(filename);
    stream.filename = filename;
    return stream;
}

function extractKey(item, keyAttributes) {
    var key = keyAttributes.reduce(function(key, attr) {
        key[attr] = item[attr];
        return key;
    }, {});

    return JSON.stringify(key);
}

function Scanner(dynamo, table, keyAttributes) {
    var key;
    var done = false;

    var scanner = {};

    scanner.read = function(callback) {
        // Makes a single scan, indexes the results by stringified key
        if (done) return callback();

        var params = { TableName: table };
        if (key) params.ExclusiveStartKey = key;

        dynamo.scan(params, function(err, data) {
            if (err) return callback(err);

            var results = data.Items.reduce(function(results, item) {
                var key = extractKey(item, keyAttributes);
                results[key] = item;
                return results;
            }, {});

            if (!data.LastEvaluatedKey) done = true;
            key = data.LastEvaluatedKey;

            callback(null, results);
        });
    };

    return scanner;
}

function run(config, callback) {
    var primaryDynamo = new AWS.DynamoDB({
        region: config.primary.region,
        accessKeyId: config.primary.accessKeyId,
        secretAccessKey: config.primary.secretAccessKey,
        sessionToken: config.primary.sessionToken,
        endpoint: config.primary.endpoint
    });

    var replicaDynamo = new AWS.DynamoDB({
        region: config.replica.region,
        accessKeyId: config.replica.accessKeyId,
        secretAccessKey: config.replica.secretAccessKey,
        sessionToken: config.replica.sessionToken,
        endpoint: config.replica.endpoint
    });

    var primaryTable = config.primary.table;
    var replicaTable = config.replica.table;

    // Setup metrics and log files
    var missing = tmpfile();
    var different = tmpfile();
    var extraneous = tmpfile();
    var discrepancies = 0;

    // Persist missing/extraneous records across scans
    var lingeringMissing = {};
    var lingeringExtraneous = {};

    // async queue for performing repairs
    var repairs = queue(config.parallel);

    primaryDynamo.describeTable({ TableName: primaryTable }, function(err, data) {
        var keyAttributes = _(data.Table.KeySchema).pluck('AttributeName');

        // Construct pseudo-streams to consume data from DynamoDB
        var primary = Scanner(primaryDynamo, primaryTable, keyAttributes);
        var replica = Scanner(replicaDynamo, replicaTable, keyAttributes);

        // Get moving
        iterate();

        function iterate() {
            queue()
                .defer(primary.read)
                .defer(replica.read)
                .await(compare);
        }

        function compare(err, primaryData, replicaData) {
            if (err) return callback(err);

            // Gather keys from the scan results
            var primaryKeys = primaryData ? Object.keys(primaryData) : [];
            var replicaKeys = replicaData ? Object.keys(replicaData) : [];

            // Items missing from the replica scan
            var missingInReplica = _(primaryKeys)
                .difference(replicaKeys)
                .map(function(key) {
                    return primaryData[key];
                });

            // Extraneous items in the replica scan
            var extraneousInReplica = _(replicaKeys)
                .difference(primaryKeys)
                .map(function(key) {
                    return replicaData[key];
                });

            // Items that differ in the two scans
            var differentInReplica = _(primaryKeys)
                .intersection(replicaKeys)
                .filter(function(key) {
                    // Do I trust this equality check?? With buffers??
                    return !_.isEqual(primaryData[key], replicaData[key]);
                })
                .map(function(key) {
                    return primaryData[key];
                });

            // Aggregate comparison results
            var result = {
                different: differentInReplica,      // primary items different in replica
                missing: missingInReplica,          // primary items missing from replica
                extraneous: extraneousInReplica     // replica items missing from primary
            };

            // Missing/Extraneous items might exist because of mismatched scan
            // lengths. These must be stashed across scan runs and checked on
            // each iteration.
            result.missing.forEach(function(missingItem) {
                lingeringMissing[extractKey(missingItem, keyAttributes)] = missingItem;
            });

            result.extraneous.forEach(function(extraneousItem) {
                lingeringExtraneous[extractKey(extraneousItem, keyAttributes)] = extraneousItem;
            });

            var missingKeys = Object.keys(lingeringMissing);
            var extraneousKeys = Object.keys(lingeringExtraneous);

            // Shared extraneous/missing keys are ones that showed up in
            // separate scan iterations. These need to be compared.
            _(missingKeys)
                .intersection(extraneousKeys)
                .forEach(function(key) {
                    var primaryItem = lingeringMissing[key];
                    var replicaItem = lingeringExtraneous[key];

                    if (!_.isEqual(primaryItem, replicaItem))
                        result.different.push(primaryItem);

                    // Different or no, these items are no longer missing/extraneous
                    delete lingeringMissing[key];
                    delete lingeringExtraneous[key];
                });

            // Different items can be written to disk and repaired at this point
            result.different.forEach(function(primaryItem) {
                discrepancies++;
                different.write(extractKey(primaryItem, keyAttributes) + '\n');

                if (config.repair) {
                    repairs.defer(replicaDynamo.putItem.bind(replicaDynamo), {
                        Item: primaryItem,
                        TableName: replicaTable
                    });
                }
            });

            // Ready for another iteration if there's anything left in either table
            if (primaryKeys.length || replicaKeys.length) iterate();
            else finish();
        }

        function finish() {
            // Log and write [optionally] any missing items
            _(lingeringMissing).each(function(primaryItem, key) {
                discrepancies++;
                missing.write(key + '\n');

                if (config.repair) {
                    repairs.defer(replicaDynamo.putItem.bind(replicaDynamo), {
                        Item: primaryItem,
                        TableName: replicaTable
                    });
                }
            });

            // Log and delete [optionally] any extraneous items
            _(lingeringExtraneous).each(function(replicaItem, key) {
                discrepancies++;
                extraneous.write(key + '\n');

                if (config.repair) {
                    repairs.defer(replicaDynamo.deleteItem.bind(replicaDynamo), {
                        Key: JSON.parse(key),
                        TableName: replicaTable
                    });
                }
            });

            repairs.awaitAll(function(err) {
                if (err) return callback(err);
                callback(null, {
                    missing: missing.filename,
                    different: different.filename,
                    extraneous: extraneous.filename,
                    discrepancies: discrepancies
                });
            });
        }
    });
}
