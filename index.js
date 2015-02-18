var kcl = require('kinesis-client-library')
var fs = require('fs');
var queue = require('queue-async');
var logger = require('fastlog')('replicator');

var writeConfig = {
    region: process.env.ReplicaRegion,
    table: process.env.ReplicaTable
};
var readConfig = {
    region: process.env.PrimaryRegion,
    table: process.env.PrimaryTable
};

var dyno = require('dyno').multi(readconfig, writeConfig);

kcl.AbstractConsumer.extend({
    processRecords: function (records, done) {

        var q = queue(10);
        records.forEach(function (record) {

            var data = JSON.parse(record.Data.toString('utf8'));
            logger.info('processing', JSON.stringify(data.dynamodb.Keys));

            q.defer(function(cb) {
                var opts = {
                    consistentRead: true,
                    table: data.table
                };
                dyno.getItem(data.dynamodb.Keys, opts, function(err, item, meta) {
                    if(err) return cb(err);

                    if(item === undefined) {
                        dyno.deleteItem(data.dynamodb.Keys, { table: data.table }, cb);
                    } else {
                        dyno.putItem(item, { table: data.table }, cb);
                    }
                });
            });
        });
        q.awaitAll(function(err, resp) {
            if(err) {
                logger.error('getItem err:', err);
                return done(err);
            }
            done(null, true);
        });
    }
});
