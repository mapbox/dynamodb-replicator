var test = require('tap').test;
var setup = require('./setup')(process.env.LIVE_TEST);
var diff = require('../diff');
var _ = require('underscore');
var config = _(setup.config).clone();
var fs = require('fs');
var util = require('util');

var opts = { timeout: 600000 };

test('setup', opts, setup.setup);

test('diff: without repairs', opts, function(assert) {
    config.repair = false;
    config.log = function() {
        config.log.messages.push(util.format.apply(this, arguments));
    };
    config.log.messages = [];

    diff(config, function(err, discrepancies) {
        assert.ifError(err, 'diff tables');
        if (err) return assert.end(err);

        console.log(config.log.messages.join('\n'));

        assert.equal(discrepancies, 4, 'four discrepacies');

        assert.deepEqual(config.log.messages, [
            'Scanning primary table and comparing to replica',
            '[missing in replica] {"hash":"hash1","range":"range1"}',
            '[different in replica] {"hash":"hash1","range":"range2"}',
            '[discrepancies] Scanning primary: 2',
            'Scanning replica table and comparing to primary',
            '[different in primary] {"hash":"hash1","range":"range2"}',
            '[missing in primary] {"hash":"hash1","range":"range3"}',
            '[discrepancies] Scanning replica: 2'
        ]);

        config.log.messages = [];
        diff(config, function(err, discrepancies) {
            assert.ifError(err, 'diff tables');
            if (err) return assert.end();

            assert.equal(discrepancies, 4, 'four discrepacies on second comparison');

            assert.deepEqual(config.log.messages, [
                'Scanning primary table and comparing to replica',
                '[missing in replica] {"hash":"hash1","range":"range1"}',
                '[different in replica] {"hash":"hash1","range":"range2"}',
                '[discrepancies] Scanning primary: 2',
                'Scanning replica table and comparing to primary',
                '[different in primary] {"hash":"hash1","range":"range2"}',
                '[missing in primary] {"hash":"hash1","range":"range3"}',
                '[discrepancies] Scanning replica: 2'
            ]);

            assert.end();
        });
    });

});

test('diff: with repairs', opts, function(assert) {
    config.repair = true;
    config.log = function() {
        config.log.messages.push(util.format.apply(this, arguments));
    };
    config.log.messages = [];

    diff(config, function(err, discrepancies) {
        assert.ifError(err, 'diff tables');
        if (err) return assert.end(err);

        assert.equal(discrepancies, 3, 'three discrepacies');

        assert.deepEqual(config.log.messages, [
            'Scanning primary table and comparing to replica',
            '[missing in replica] {"hash":"hash1","range":"range1"}',
            '[different in replica] {"hash":"hash1","range":"range2"}',
            '[discrepancies] Scanning primary: 2',
            'Scanning replica table and comparing to primary',
            '[missing in primary] {"hash":"hash1","range":"range3"}',
            '[discrepancies] Scanning replica: 1'
        ]);

        config.repair = false;
        config.log.messages = [];
        diff(config, function(err, discrepancies) {
            assert.ifError(err, 'diff tables');
            if (err) return assert.end();

            assert.equal(discrepancies, 0, 'no discrepacies on second comparison');

            assert.deepEqual(config.log.messages, [
                'Scanning primary table and comparing to replica',
                '[discrepancies] Scanning primary: 0',
                'Scanning replica table and comparing to primary',
                '[discrepancies] Scanning replica: 0'
            ]);

            assert.end();
        });
    });
});

test('teardown', opts, setup.teardown);
