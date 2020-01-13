module.exports = {
    AttributeDefinitions: [
        {AttributeName: 'id', AttributeType: 'S'},
        {AttributeName: 'arange', AttributeType: 'N'}
    ],
    KeySchema: [
        {AttributeName: 'id', KeyType: 'HASH'},
        {AttributeName: 'arange', KeyType: 'RANGE'}
    ],
    ProvisionedThroughput: {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 1
    }
};
