var Dyno = require('dyno');

module.exports = function(records) {
    return records.map(function(record) {
        return {
            EventName: 'INSERT',
            EventVersion: '1.0',
            EventSource: 'aws:dynamodb',
            Dynamodb: {
                NewImage: JSON.parse(Dyno.serialize(record)),
                SizeBytes: JSON.stringify(record).length,
                StreamViewType: 'NEW_AND_OLD_IMAGES',
                Keys: {
                    id: JSON.parse(Dyno.serialize(record)).id
                }
            },
            EventID: '1',
            eventSourceARN: 'localhost:4567',
            AwsRegion: 'us-east-1'
        };
    });
}
