var test = require('tape'),
    fs = require('fs'),
    queue = require('queue-async');
var s = require('./setup')();
var config = s.config;
var dynos = s.dynos;


var consumer = require('../')(config);

var data = [
    {
        "awsRegion": "us-east-1",
        "dynamodb": {
            "Keys": {
                "hash": {"S": "hash1" },
                "range": {"S": "range1" }
            },
            "SequenceNumber": "300000000000000499659",
            "SizeBytes": 41,
            "StreamViewType": "KEYS_ONLY"
        },
        "eventID": "e2fd9c34eff2d779b297b26f5fef4206",
        "eventName": "INSERT",
        "eventSource": "aws:dynamodb",
        "eventVersion": "1.0"
    },
    {
            "awsRegion": "us-east-1",
            "dynamodb": {
                "Keys": {
                    "hash": {"S": "hash1"},
                    "range": {"S": "range2"}
                },
                "SequenceNumber": "300000000000000499659",
                "SizeBytes": 41,
                "StreamViewType": "KEYS_ONLY"
            },
            "eventID": "e2fd9c34eff2d779b297b26f5fef4206",
            "eventName": "INSERT",
            "eventSource": "aws:dynamodb",
            "eventVersion": "1.0"
    },
    {
            "awsRegion": "us-east-1",
            "dynamodb": {
                "Keys": {
                    "hash": {"S": "hash1"},
                    "range": {"S": "range3"}
                },
                "SequenceNumber": "300000000000000499674",
                "SizeBytes": 41,
                "StreamViewType": "KEYS_ONLY"
            },
            "eventID": "e2fd9c34eff2d779b297b26f5fef4207",
            "eventName": "DELETE",
            "eventSource": "aws:dynamodb",
            "eventVersion": "1.0"
    }
];


var records = data.map(function(d){
    return {Data: JSON.stringify(d)}
});



test('setup', s.setup);
test('processRecords', function(t) {
    consumer.processRecords(records, function(err, res){
        t.notOk(err);
        t.equal(res, true, 'was successful');
        t.end();
    });
});

test('processRecords - replicated items', function(t) {
    var q = queue();
    q.defer(dynos.replica.getItem, {range: 'range1', hash: 'hash1'});
    q.defer(dynos.replica.getItem, {range: 'range2', hash: 'hash1'});
    q.defer(dynos.replica.getItem, {range: 'range3', hash: 'hash1'});
    q.awaitAll(function(err, items) {
        t.notOk(err);
        t.deepEquals(items[0], {range: 'range1', hash: 'hash1', other:1}, 'replica has the same item');
        t.deepEquals(items[1], {range: 'range2', hash: 'hash1', other:2}, 'replica updated item');
        t.equals(items[2], undefined, 'replica deleted an item');
        t.end();
    });
});

test('teardown', s.teardown);
