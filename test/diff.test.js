var test = require('tap').test;
var setup = require('./setup')(process.env.LIVE_TEST);
var diff = require('../diff');
var _ = require('underscore');
var fs = require('fs');
var util = require('util');

var opts = { timeout: 600000 };

var config = _(setup.config).clone();
config.log = function() {
    config.log.messages.push(util.format.apply(this, arguments));
};
config.log.messages = [];

test('setup', opts, setup.setup);

test('diff: without repairs', opts, function(assert) {
    config.repair = false;
    config.log.messages = [];

    diff(config, function(err, discrepancies) {
        assert.ifError(err, 'diff tables');
        if (err) return assert.end(err);

        assert.equal(discrepancies, 4, 'four discrepacies');

        assert.deepEqual(config.log.messages, [
            'Scanning primary table and comparing to replica',
            '[missing] {"hash":"hash1","range":"range1"}',
            '[different] {"hash":"hash1","range":"range2"}',
            '[discrepancies] 2',
            'Scanning replica table and comparing to primary',
            '[different] {"hash":"hash1","range":"range2"}',
            '[extraneous] {"hash":"hash1","range":"range3"}',
            '[discrepancies] 2'
        ]);

        config.log.messages = [];
        diff(config, function(err, discrepancies) {
            assert.ifError(err, 'diff tables');
            if (err) return assert.end();

            assert.equal(discrepancies, 4, 'four discrepacies on second comparison');

            assert.deepEqual(config.log.messages, [
                'Scanning primary table and comparing to replica',
                '[missing] {"hash":"hash1","range":"range1"}',
                '[different] {"hash":"hash1","range":"range2"}',
                '[discrepancies] 2',
                'Scanning replica table and comparing to primary',
                '[different] {"hash":"hash1","range":"range2"}',
                '[extraneous] {"hash":"hash1","range":"range3"}',
                '[discrepancies] 2'
            ]);

            assert.end();
        });
    });

});

test('diff: with repairs', opts, function(assert) {
    config.repair = true;
    config.log.messages = [];

    diff(config, function(err, discrepancies) {
        assert.ifError(err, 'diff tables');
        if (err) return assert.end(err);

        assert.equal(discrepancies, 3, 'three discrepacies');

        assert.deepEqual(config.log.messages, [
            'Scanning primary table and comparing to replica',
            '[missing] {"hash":"hash1","range":"range1"}',
            '[different] {"hash":"hash1","range":"range2"}',
            '[discrepancies] 2',
            'Scanning replica table and comparing to primary',
            '[extraneous] {"hash":"hash1","range":"range3"}',
            '[discrepancies] 1'
        ]);

        config.repair = false;
        config.log.messages = [];
        diff(config, function(err, discrepancies) {
            assert.ifError(err, 'diff tables');
            if (err) return assert.end();

            assert.equal(discrepancies, 0, 'no discrepacies on second comparison');

            assert.deepEqual(config.log.messages, [
                'Scanning primary table and comparing to replica',
                '[discrepancies] 0',
                'Scanning replica table and comparing to primary',
                '[discrepancies] 0'
            ]);

            assert.end();
        });
    });
});
test('teardown', setup.teardown);

test('setup', opts, setup.setup);
test('diff: backfill', opts, function(assert) {
    config.repair = true;
    config.backfill = true;
    config.log.messages = [];

    diff(config, function(err, discrepancies) {
        assert.ifError(err, 'diff tables');
        if (err) return assert.end();

        assert.equal(discrepancies, 2, 'two discrepacies');
        assert.deepEqual(config.log.messages, [
            'Scanning primary table and comparing to replica',
            '[missing] {"hash":"hash1","range":"range1"}',
            '[different] {"hash":"hash1","range":"range2"}',
            '[discrepancies] 2'
        ]);

        config.repair = false;
        config.log.messages = [];
        diff(config, function(err, discrepancies) {
            assert.ifError(err, 'diff tables');
            if (err) return assert.end();

            assert.equal(discrepancies, 0, 'no discrepacies on second comparison');
            assert.deepEqual(config.log.messages, [
                'Scanning primary table and comparing to replica',
                '[discrepancies] 0'
            ]);

            assert.end();
        });
    });
});
test('teardown', setup.teardown);

test('setup', opts, setup.setup);
test('diff: parallel', opts, function(assert) {
    config.repair = false;
    config.backfill = false;
    config.segment = 0;
    config.segments = 10;
    config.log.messages = [];

    setup.differentItemsPlease(1000, function(err) {
        if (err) throw err;

        diff(config, function(err, discrepancies) {
            assert.ifError(err, 'diff tables');
            if (err) return assert.end();

            assert.ok(discrepancies < 1000, 'scanned partial table');
            assert.end();
        });
    });
});

test('teardown', opts, setup.teardown);
