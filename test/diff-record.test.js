var setup = require('./setup')(true);
var exec = require('child_process').exec;
var test = require('tape');
var queue = require('queue-async');
var diffRecord = require('path').resolve(__dirname, '..', 'bin', 'diff-record.js');

test('setup', setup.setup);
test('diff-record', function(assert) {
    queue()
        .defer(function(next) {
            var cmd = [
                diffRecord,
                setup.config.primary.region + '/' + setup.config.primary.table,
                setup.config.replica.region + '/' + setup.config.replica.table,
                '\'{"hash":"hash1","range":"range2"}\''
            ].join(' ');
            exec(cmd, function(err, stdout, stderr) {
                assert.ifError(err, '[different] does not error');
                assert.ok(/✘/.test(stdout), '[different] reports difference');
                next();
            });
        })
        .defer(function(next) {
            var cmd = [
                diffRecord,
                setup.config.primary.region + '/' + setup.config.primary.table,
                setup.config.replica.region + '/' + setup.config.replica.table,
                '\'{"hash":"hash1","range":"range4"}\''
            ].join(' ');
            exec(cmd, function(err, stdout, stderr) {
                assert.ifError(err, '[equivalent] does not error');
                assert.ok(/✔/.test(stdout), '[equivalent] reports equivalence');
                next();
            });
        })
        .awaitAll(function() {
            assert.end();
        });
});
test('teardown', setup.teardown);
