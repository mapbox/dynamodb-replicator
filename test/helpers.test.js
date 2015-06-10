var test = require('tape');
var helpers = require('../dynostream-replicator').helpers;
var testEventSingleRecord = require('./fixtures/test-event-single-record.json');
var _ = require('underscore');

test('actionsPerId: returns correct for single record', function(assert) {
    var key = JSON.stringify({
        range: {
            N:'2938727'
        },
        hash: {
            S:'2015-06-08T20:01:31.345Z_f47b0860-0f07-4660-a097-8bcdb3c16f75'
        }
    });
    var eventsById = helpers.actionsPerId(testEventSingleRecord.Records);
    assert.deepEqual(_.keys(eventsById), [key], 'Only has one key');
    assert.equal(eventsById[key].length, 3, 'Has three events for the key');

    var result = {};
    result[key] = testEventSingleRecord.Records;
    assert.deepEqual(eventsById, result, 'All events for the key are associated with the key');
    assert.end();
});

