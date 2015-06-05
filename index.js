var Dyno = require('dyno');
var queue = require('queue-async');
var streambot = require('streambot');

module.exports = replicate;
module.exports.streambot = streambot(replicate);

function replicate(records, callback) {
    var primaryConfig = {
        region: process.env.PrimaryRegion,
        table: process.env.PrimaryTable
    };

    if (process.env.PrimaryEndpoint) primaryConfig.endpoint = process.env.PrimaryEndpoint;
    var primary = Dyno(primaryConfig);

    var replicaConfig = {
        region: process.env.ReplicaRegion,
        table: process.env.ReplicaTable
    };

    if (process.env.ReplicaEndpoint) replicaConfig.endpoint = process.env.ReplicaEndpoint;
    var replica = Dyno(replicaConfig);

    var q = queue();

    records.forEach(function(record) {
        q.defer(function(next) {
            var data;
            try { data = JSON.parse(record.data); }
            catch (err) { return next(err); }

            // @rclark: this is what I mean. Very hacky.
            var key = Dyno.deserialize(JSON.stringify(data.dynamodb.Keys));

            streambot.log.info('Processing %j', key);

            primary.getItem(key, {consistentRead: true}, function(err, item) {
                if (err) return next(err);

                if (!item) replica.deleteItem(data.dynamodb.Keys, next);
                else replica.putItem(item, next);
            });
        });
    });

    q.awaitAll(callback);
}
