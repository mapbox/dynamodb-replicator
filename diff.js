var fs = require('fs');
var _ = require('underscore');
var queue = require('queue-async');
var es = require('event-stream');
var Dyno = require('dyno');
var path = require('path');
var os = require('os');
var crypto = require('crypto');

function tmpfile() {
    var filename = path.join(os.tmpdir(), crypto.randomBytes(8).toString('hex'));
    var stream = fs.createWriteStream(filename);
    stream.filename = filename;
    return stream;
}

module.exports = function(config, callback) {
    var primary = Dyno(config.primary);
    var replica = Dyno(config.replica);

    var q = queue(config.parallel || 3);
    var errors = tmpfile();
    var missing = tmpfile();
    var different = tmpfile();
    var discrepancies = 0;

    primary.describeTable(function(err, description) {
        if (err) return callback(err);

        var keySchema = _(description.Table.KeySchema).pluck('AttributeName');

        primary.scan()
            .pipe(es.map(function(record, cb) {
                var key = keySchema.reduce(function(key, attribute) {
                    key[attribute] = record[attribute];
                    return key;
                }, {});

                replica.getItem(key, function(err, resp) {
                    if (err) {
                        errors.write(err + '\n');
                    } else if (!resp) {
                        discrepancies++;
                        missing.write(JSON.stringify(key) + '\n');
                        if (config.repair) q.defer(replica.putItem, record);
                    } else if (!_.isEqual(record, resp)) {
                        discrepancies++;
                        different.write(JSON.stringify(key) + '\n');
                        if (config.repair) q.defer(replica.putItem, record);
                    }

                    cb();
                });
            }))
            .on('error', callback)
            .on('end', function() {
                [errors, missing, different].forEach(function(output) {
                    q.defer(function(next) {
                        output.on('finish', next);
                        output.end();
                    });
                });

                q.awaitAll(function(err) {
                    if (err) return callback(err);

                    callback(null, {
                        errors: errors.filename,
                        missing: missing.filename,
                        different: different.filename,
                        discrepancies: discrepancies
                    });
                });
            });
    });
};
