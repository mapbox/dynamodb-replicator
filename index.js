var fs = require('fs');
var queue = require('queue-async');
var logger = require('fastlog')('replicator');

/*

 Returns an object that can be used to extend kcl.AbstractConsumer

 Config needs to include the dyno config for a primary and replica

 {
    primary: {
      region: 'us-east-1',
      table: 'test-primary'
    },
    replica: {
      region: 'eu-west-1',
      table: 'test-replica'
    }
 }

 Dynamo items will be read from the primary, and written to the replica.

*/


module.exports = function(config){

    config.logging = config.logging || true;

    var dynoPrimary = require('dyno')(config.primary)
    var dynoReplica = require('dyno')(config.replica);

    return {
        processRecords: function (records, done) {

            var q = queue();
            records.forEach(function (record) {

                var data = JSON.parse(record.Data.toString('utf8'));
                if(config.logging) logger.info('processing', JSON.stringify(data.dynamodb.Keys));

                q.defer(function(cb) {
                    dynoPrimary.getItem(data.dynamodb.Keys, {consistentRead: true}, function(err, item, meta) {
                        if(err) return cb(err);

                        if(item === undefined) {
                            dynoReplica.deleteItem(data.dynamodb.Keys, { table: data.table }, cb);
                        } else {
                            dynoReplica.putItem(item, cb);
                        }
                    });
                });
            });
            q.awaitAll(function(err, resp) {
                if(err) {
                    if(config.logging) logger.error('getItem err:', err);
                    return done(err);
                }
                done(null, true);
            });
        }
    };
}
