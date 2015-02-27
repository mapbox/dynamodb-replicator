var test = require('tape');
var setup = require('./setup')(process.env.LIVE_TEST);
var diff = require('../diff');
var _ = require('underscore');
var config = _(setup.config).clone();
var fs = require('fs');
var exec = require('child_process').exec;

var opts = { timeout: 600000 };

test('setup', opts, setup.setup);

test('diff: without repairs', opts, function(assert) {
    diff(config, function(err, results) {
        assert.ifError(err, 'diff tables');
        if (err) return assert.end();

        assert.equal(results.discrepancies, 3, 'three discrepacies');

        var extraneous = fs.readFileSync(results.extraneous, 'utf8');
        var missing = fs.readFileSync(results.missing, 'utf8');
        var different = fs.readFileSync(results.different, 'utf8');

        assert.equal(extraneous, '{"hash":{"S":"hash1"},"range":{"S":"range3"}}\n', 'expected extraneous record');
        assert.equal(missing, '{"hash":{"S":"hash1"},"range":{"S":"range1"}}\n', 'expected missing record');
        assert.equal(different, '{"hash":{"S":"hash1"},"range":{"S":"range2"}}\n', 'expected different record');

        assert.end();
    });
});

test('diff: with repairs', opts, function(assert) {
    config.repair = true;

    diff(config, function(err, results) {
        assert.ifError(err, 'diff tables');
        if (err) return assert.end();

        assert.equal(results.discrepancies, 3, 'three discrepacies');

        var extraneous = fs.readFileSync(results.extraneous, 'utf8');
        var missing = fs.readFileSync(results.missing, 'utf8');
        var different = fs.readFileSync(results.different, 'utf8');

        assert.equal(extraneous, '{"hash":{"S":"hash1"},"range":{"S":"range3"}}\n', 'expected extraneous record');
        assert.equal(missing, '{"hash":{"S":"hash1"},"range":{"S":"range1"}}\n', 'expected missing record');
        assert.equal(different, '{"hash":{"S":"hash1"},"range":{"S":"range2"}}\n', 'expected different record');

        config.repair = false;
        diff(config, function(err, results) {
            assert.ifError(err, 'diff tables');
            if (err) return assert.end();

            assert.equal(results.discrepancies, 0, 'no discrepacies');

            var extraneous = fs.readFileSync(results.extraneous, 'utf8');
            var missing = fs.readFileSync(results.missing, 'utf8');
            var different = fs.readFileSync(results.different, 'utf8');

            assert.notOk(extraneous, 'no extraneous logged');
            assert.notOk(missing, 'no missing logged');
            assert.notOk(different, 'no different logged');

            assert.end();
        });
    });
});

test('teardown', opts, setup.teardown);
