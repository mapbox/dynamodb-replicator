var test = require('tape');
var AWS = require('aws-sdk');
var s3 = new AWS.S3();
var crypto = require('crypto');
var tableDef = require('./fixtures/table');
var dynamodb = require('dynamodb-test')(test, 's3-backfill', tableDef);
var Dyno = require('dyno');
var queue = require('queue-async');
var zlib = require('zlib');
var path = require('path');
var os = require('os');
var fs = require('fs');

var backfill = require('../s3-backfill');
var snapshot = require('../s3-snapshot');

var bucket = process.env.BackupBucket;
var prefix = 'dynamodb-replicator/test/' + crypto.randomBytes(4).toString('hex');
var records = require('./fixtures/records')(50);

dynamodb.test('[s3-backfill]', records, function(assert) {

    var config = {
        table: dynamodb.tableName,
        region: 'fake',
        endpoint: 'http://localhost:4567',
        accessKey: 'fake',
        secretAccessKey: 'fake',
        backup: {
            bucket: bucket,
            prefix: prefix
        }
    };

    backfill(config, function(err) {
        console.log('\n');
        assert.ifError(err, 'success');

        s3.listObjects({
            Bucket: bucket,
            Prefix: prefix
        }, function(err, data) {
            if (err) throw err;
            checkS3(data.Contents.map(function(item) {
                return item.Key;
            }));
        });
    });

    function checkS3(keys) {
        assert.equal(keys.length, 50, 'all records written to S3');
        var q = queue(20);

        records.forEach(function(expected) {
            var key = crypto.createHash('md5')
                .update(Dyno.serialize({ id: expected.id }))
                .digest('hex');

            key = [prefix, dynamodb.tableName, key].join('/');
            expected = Dyno.serialize(expected);

            assert.ok(keys.indexOf(key) > -1, 'expected item written for ' + key);
            q.defer(function(next) {
                s3.getObject({
                    Bucket: bucket,
                    Key: key
                }, function(err, data) {
                    if (err) throw err;
                    assert.equal(data.Body.toString(), expected, 'expected data for ' + key);
                    next();
                });
            });
        });

        q.awaitAll(function() {
            assert.end();
        });
    }
});

test('[s3-snapshot]', function(assert) {
    var snapshotKey = [prefix, dynamodb.tableName, 'snapshot'].join('/');
    var log = path.join(os.tmpdir(), crypto.randomBytes(16).toString('hex'));

    var config = {
        source: {
            bucket: bucket,
            prefix: [prefix, dynamodb.tableName].join('/')
        },
        destination: {
            bucket: bucket,
            key: snapshotKey
        },
        logger: fs.createWriteStream(log)
    };

    snapshot(config, function(err, details) {
        assert.ifError(err, 'success');
        assert.equal(details.count, 50, 'reported 50 items');
        assert.ok(details.size, 'reported size');

        var result = '';
        var gunzip = zlib.createGunzip()
            .on('readable', function() {
                var d = gunzip.read();
                if (d) result += d;
            })
            .on('end', function() {
                checkFile(result);
            });

        s3.getObject({
            Bucket: bucket,
            Key: snapshotKey
        }).createReadStream().pipe(gunzip);
    });

    function checkFile(found) {
        found = found.trim().split('\n').map(function(line) {
            return JSON.parse(line);
        });

        var expected = records.reduce(function(expected, item) {
            expected[item.id] = Dyno.serialize(item);
            return expected;
        }, {});

        assert.equal(found.length, 50, 'all objects snapshotted');

        found.forEach(function(item) {
            var id = item.id.S;
            var original = expected[id];
            assert.equal(JSON.stringify(item), original, 'expected item in snapshot ' + id);
        });
        checkLog();
    }

    function checkLog() {
        fs.readFile(log, 'utf8', function(err, data) {
            assert.ifError(err, 'log file was written');
            assert.ok(data.length, 'has logs in it');
            assert.end();
        });
    }
});

dynamodb.close();
