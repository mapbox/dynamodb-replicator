var test = require('tap').test;
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

        assert.equal(results.discrepancies, 2, 'two discrepacies');

        var errors = fs.readFileSync(results.errors, 'utf8');
        var missing = fs.readFileSync(results.missing, 'utf8');
        var different = fs.readFileSync(results.different, 'utf8');

        assert.notOk(errors, 'no errors logged');
        assert.equal(missing, '{"hash":"hash1","range":"range1"}\n', 'expected missing record');
        assert.equal(different, '{"hash":"hash1","range":"range2"}\n', 'expected different record');

        assert.end();
    });
});

test('diff: with repairs', opts, function(assert) {
    config.repair = true;

    diff(config, function(err, results) {
        assert.ifError(err, 'diff tables');
        if (err) return assert.end();

        assert.equal(results.discrepancies, 2, 'two discrepacies');

        var errors = fs.readFileSync(results.errors, 'utf8');
        var missing = fs.readFileSync(results.missing, 'utf8');
        var different = fs.readFileSync(results.different, 'utf8');

        assert.notOk(errors, 'no errors logged');
        assert.equal(missing, '{"hash":"hash1","range":"range1"}\n', 'expected missing record');
        assert.equal(different, '{"hash":"hash1","range":"range2"}\n', 'expected different record');

        config.repair = false;
        diff(config, function(err, results) {
            assert.ifError(err, 'diff tables');
            if (err) return assert.end();

            assert.equal(results.discrepancies, 0, 'no discrepacies');

            var errors = fs.readFileSync(results.errors, 'utf8');
            var missing = fs.readFileSync(results.missing, 'utf8');
            var different = fs.readFileSync(results.different, 'utf8');

            assert.notOk(errors, 'no errors logged');
            assert.notOk(missing, 'no missing logged');
            assert.notOk(different, 'no different logged');

            assert.end();
        });
    });
});

test('teardown', opts, setup.teardown);
