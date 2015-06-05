var AWS = require('aws-sdk');
var queue = require('queue-async');
var streambot = require('streambot');

module.exports = replicate;
module.exports.streambot = streambot(replicate);

function replicate(records, callback) {
    var primaryConfig = { region: process.env.PrimaryRegion };
    if (process.env.PrimaryEndpoint) primaryConfig.endpoint = process.env.PrimaryEndpoint;
    var primary = new AWS.DynamoDB(primaryConfig);

    var replicaConfig = { region: process.env.ReplicaRegion };
    if (process.env.ReplicaEndpoint) replicaConfig.endpoint = process.env.ReplicaEndpoint;
    var replica = new AWS.DynamoDB(replicaConfig);

    var q = queue();

    records.forEach(function(record) {
        q.defer(function(next) {
            var data;
            try { data = JSON.parse(record.data); }
            catch (err) { return next(err); }

            var getParams = {
                TableName: process.env.PrimaryTable,
                Key: data.dynamodb.Keys,
                ConsistentRead: true
            };

            streambot.log.info('Processing %j', getParams.Key);

            primary.getItem(getParams, function(err, response) {
                if (err) return next(err);

                if (!response.Item) return replica.deleteItem({
                    TableName: process.env.ReplicaTable,
                    Key: data.dynamodb.Keys
                }, next);

                var putParams = {
                    TableName: process.env.ReplicaTable,
                    Item: response.Item
                };

                replica.putItem(putParams, next);
            });
        });
    });

    q.awaitAll(callback);
}
