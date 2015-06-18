var AWS = require('aws-sdk');
var queue = require('queue-async');
var streambot = require('streambot');

module.exports = replicate;
module.exports.streambot = streambot(replicate);

function replicate(event, callback) {
    console.log('Env: %s', JSON.stringify(process.env));

    var primaryConfig = { region: process.env.PrimaryRegion };
    if (process.env.PrimaryEndpoint) primaryConfig.endpoint = process.env.PrimaryEndpoint;
    var primary = new AWS.DynamoDB(primaryConfig);

    var replicaConfig = { region: process.env.ReplicaRegion };
    if (process.env.ReplicaEndpoint) replicaConfig.endpoint = process.env.ReplicaEndpoint;
    var replica = new AWS.DynamoDB(replicaConfig);

    var q = queue();

    event.Records
        .filter(function(record) {
            return record.eventName === 'aws:kinesis:record';
        })
        .forEach(function(record) {
            record.kinesis.data = new Buffer(record.kinesis.data, 'base64').toString();
            record = record.kinesis;

            q.defer(function(next) {
                var data;
                try { data = JSON.parse(record.data); }
                catch (err) { return next(err); }

                var getParams = {
                    TableName: process.env.PrimaryTable,
                    Key: data.dynamodb.Keys,
                    ConsistentRead: true
                };

                console.log('Processing: %s', JSON.stringify(getParams.Key));
                console.log('GET: %s', JSON.stringify(getParams));

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

                    console.log('PUT: %s', JSON.stringify(putParams));
                    replica.putItem(putParams, next);
                });
            });
    });

    q.awaitAll(callback);
}
