var Dyno = require('dyno');

module.exports = function(action, records) {
    switch(action) {
        case 'INSERT':
            return records.map(insertRecord);
            break;
        case 'MODIFY':
            return records.map(modifyRecord);
            break;
        case 'REMOVE':
            return records.map(removeRecord);
            break;
        default:
            return;
            break;
    }

    function insertRecord(record) {
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
            EventID: '' + Math.floor(Math.random() * 10),
            eventSourceARN: 'localhost:4567',
            AwsRegion: 'us-east-1'
        };
    }

    function modifyRecord(record) {
        return {
            EventName: 'MODIFY',
            EventVersion: '1.0',
            EventSource: 'aws:dynamodb',
            Dynamodb: {
                NewImage: JSON.parse(Dyno.serialize(record)),
                OldImage: JSON.parse(Dyno.serialize(record)),
                SizeBytes: JSON.stringify(record).length,
                StreamViewType: 'NEW_AND_OLD_IMAGES',
                Keys: {
                    id: JSON.parse(Dyno.serialize(record)).id
                }
            },
            EventID: '' + Math.floor(Math.random() * 10),
            eventSourceARN: 'localhost:4567',
            AwsRegion: 'us-east-1'
        };
    }

    function removeRecord(record) {
        return {
            EventName: 'REMOVE',
            EventVersion: '1.0',
            EventSource: 'aws:dynamodb',
            Dynamodb: {
                OldImage: JSON.parse(Dyno.serialize(record)),
                SizeBytes: JSON.stringify(record).length,
                StreamViewType: 'NEW_AND_OLD_IMAGES',
                Keys: {
                    id: JSON.parse(Dyno.serialize(record)).id
                }
            },
            EventID: '' + Math.floor(Math.random() * 10),
            eventSourceARN: 'localhost:4567',
            AwsRegion: 'us-east-1'
        };
    }
}
