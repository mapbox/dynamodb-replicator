var test = require('tape');
var primary = require('dynamodb-test')(test, 'dynamodb-replicator', require('./table.json'));
var replica = require('dynamodb-test')(test, 'dynamodb-replicator', require('./table.json'));
var diff = require('../diff');
var util = require('util');
var _ = require('underscore');
var crypto = require('crypto');
var parse_location = require('../parse-location')

var primaryItems = [
    {hash: 'hash1', range: 'range1', other:1},
    {hash: 'hash1', range: 'range2', other:2},
    {hash: 'hash1', range: 'range4', other: new Buffer('hello world')}
];

var replicaItems = [
    {hash: 'hash1', range: 'range2', other:10000},
    {hash: 'hash1', range: 'range3', other:3},
    {hash: 'hash1', range: 'range4', other: new Buffer('hello world')}
];

var records = _.range(1000).map(function() {
    return {
        hash: crypto.randomBytes(8).toString('hex'),
        range: crypto.randomBytes(8).toString('hex'),
        other: crypto.randomBytes(8)
    };
});

var config = {
    primary: {
        table: primary.tableName,
        region: 'fake',
        endpoint: 'http://localhost:4567'
    },
    replica: {
        table: replica.tableName,
        region: 'fake',
        endpoint: 'http://localhost:4567'
    },
    log: function() {
        config.log.messages.push(util.format.apply(this, arguments));
    }
};

config.log.messages = [];

primary.start();
replica.start();

primary.load(primaryItems);
replica.load(replicaItems);

test('diff: without repairs', function(assert) {
    config.repair = false;
    config.log.messages = [];

    diff(config, function(err, discrepancies) {
        assert.ifError(err, 'diff tables');
        if (err) return assert.end(err);

        assert.equal(discrepancies, 4, 'four discrepacies');

        assert.equal(_.difference(config.log.messages, [
            'Scanning primary table and comparing to replica',
            '[missing] {"hash":"hash1","range":"range1"}',
            '[different] {"hash":"hash1","range":"range2"}',
            '[discrepancies] 2',
            'Scanning replica table and comparing to primary',
            '[extraneous] {"hash":"hash1","range":"range3"}',
            '[different] {"hash":"hash1","range":"range2"}',
            '[discrepancies] 2',
            '[progress] Scan rate: 6 items @ 6 items/s | Compare rate: 6 items/s'
        ]).length, 0, 'expected log messages');

        config.log.messages = [];
        diff(config, function(err, discrepancies) {
            assert.ifError(err, 'diff tables');
            if (err) return assert.end();

            assert.equal(discrepancies, 4, 'four discrepacies on second comparison');

            assert.equal(_.difference(config.log.messages, [
                'Scanning primary table and comparing to replica',
                '[missing] {"hash":"hash1","range":"range1"}',
                '[different] {"hash":"hash1","range":"range2"}',
                '[discrepancies] 2',
                'Scanning replica table and comparing to primary',
                '[extraneous] {"hash":"hash1","range":"range3"}',
                '[different] {"hash":"hash1","range":"range2"}',
                '[discrepancies] 2',
                '[progress] Scan rate: 6 items @ 6 items/s | Compare rate: 6 items/s'
            ]).length, 0, 'expected log messages');

            assert.end();
        });
    });

});

test('diff: with repairs', function(assert) {
    config.repair = true;
    config.log.messages = [];

    diff(config, function(err, discrepancies) {
        assert.ifError(err, 'diff tables');
        if (err) return assert.end(err);

        assert.equal(discrepancies, 3, 'three discrepacies');

        assert.equal(_.difference(config.log.messages, [
            'Scanning primary table and comparing to replica',
            '[missing] {"hash":"hash1","range":"range1"}',
            '[different] {"hash":"hash1","range":"range2"}',
            '[discrepancies] 2',
            'Scanning replica table and comparing to primary',
            '[extraneous] {"hash":"hash1","range":"range3"}',
            '[discrepancies] 1',
            '[progress] Scan rate: 7 items @ 7 items/s | Compare rate: 7 items/s'
        ]).length, 0, 'expected log messages');

        config.repair = false;
        config.log.messages = [];
        diff(config, function(err, discrepancies) {
            assert.ifError(err, 'diff tables');
            if (err) return assert.end();

            assert.equal(discrepancies, 0, 'no discrepacies on second comparison');

            assert.equal(_.difference(config.log.messages, [
                'Scanning primary table and comparing to replica',
                '[discrepancies] 0',
                'Scanning replica table and comparing to primary',
                '[discrepancies] 0',
                '[progress] Scan rate: 6 items @ 6 items/s | Compare rate: 6 items/s'
            ]).length, 0, 'expected log messages');

            assert.end();
        });
    });
});

primary.empty();
replica.empty();
primary.load(primaryItems);
replica.load(replicaItems);

test('diff: backfill', function(assert) {
    config.repair = true;
    config.backfill = true;
    config.log.messages = [];

    diff(config, function(err, discrepancies) {
        assert.ifError(err, 'diff tables');
        if (err) return assert.end();

        assert.equal(discrepancies, 3, 'three records backfilled');
        assert.deepEqual(config.log.messages, [
            'Scanning primary table and comparing to replica',
            '[backfill] {"hash":"hash1","range":"range1"}',
            '[backfill] {"hash":"hash1","range":"range2"}',
            '[backfill] {"hash":"hash1","range":"range4"}',
            '[discrepancies] 3',
            '[progress] Scan rate: 3 items @ 3 items/s | Compare rate: 3 items/s'
        ]);

        config.repair = false;
        config.backfill = false;
        config.log.messages = [];
        diff(config, function(err, discrepancies) {
            assert.ifError(err, 'diff tables');
            if (err) return assert.end();

            assert.equal(discrepancies, 1, 'only an extraneous discrepacy on second comparison');
            assert.deepEqual(config.log.messages, [
                'Scanning primary table and comparing to replica',
                '[discrepancies] 0',
                'Scanning replica table and comparing to primary',
                '[extraneous] {"hash":"hash1","range":"range3"}',
                '[discrepancies] 1',
                '[progress] Scan rate: 7 items @ 7 items/s | Compare rate: 7 items/s'
            ]);

            assert.end();
        });
    });
});

primary.empty();
replica.empty();
primary.load(records);

test('diff: repair/backfill large number of discrepancies', function(assert) {
    config.repair = true;
    config.backfill = true;
    config.log.messages = [];

    diff(config, function(err, discrepancies) {
        assert.ifError(err, 'diff tables');
        if (err) return assert.end();
        assert.equal(discrepancies, 1000, '1000 records backfilled');

        config.repair = false;
        config.backfill = false;
        config.log.messages = [];
        diff(config, function(err, discrepancies) {
            assert.ifError(err, 'diff tables');
            if (err) return assert.end();

            assert.equal(discrepancies, 0, '0 discrepancies post-backfill');
            assert.end();
        });
    });
});

primary.empty();
replica.empty();
primary.load(records);

test('diff: parallel', function(assert) {
    config.repair = false;
    config.backfill = false;
    config.segment = 0;
    config.segments = 10;
    config.log.messages = [];

    diff(config, function(err, discrepancies) {
        assert.ifError(err, 'diff tables');
        if (err) return assert.end();

        assert.ok(discrepancies < 1000, 'scanned partial table');
        assert.end();
    });
});

primary.delete();
replica.delete();
primary.close();

test('diff: parsing locations', function(assert) {
    // Testing with local region and endpoint URL
    primary = '127.0.0.1:8000/table1', replica = 'localhost:8000/table2';
    primary = primary.split('/'), replica = replica.split('/');
    var locations = parse_location.parse(primary, replica);
    primary = locations[0], replica = locations[1];
    assert.ok(primary['endpoint']=='http://127.0.0.1:8000' && primary['region']=='local', 
        'got region and endpoint from local ip');
    assert.ok(replica['endpoint']=='http://localhost:8000' && replica['region']=='local', 
        'got region and endpoint from localhost');

    // Testing with valid AWS region (Using Beijing region)
    primary = 'cn-north-1/table1', replica = 'cn-north-1/table2';
    primary = primary.split('/'), replica = replica.split('/');
    locations = parse_location.parse(primary, replica);
    primary = locations[0], replica = locations[1];
    assert.ok(primary['endpoint']==null && primary['region']=='cn-north-1' && primary['table']=='table1', 
        'got endpoint, region and table from AWS region');
    assert.ok(replica['endpoint']==null && replica['region']=='cn-north-1' && replica['table']=='table2', 
        'got endpoint, region and table from AWS region');
    assert.end()
});