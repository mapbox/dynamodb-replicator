var test = require('tape');
var { S3Client, ListObjectsCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
var { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
var s3Client = new S3Client({
    region: 'us-east-1',
    credentials: {
        accessKeyId: 'fake',
        secretAccessKey: 'fake'
    }
});
var crypto = require('crypto');
var tableDef = require('./fixtures/table');
var dynamodb = require('@mapbox/dynamodb-test')(test, 's3-backfill', tableDef);
var Dyno = require('@mapbox/dyno');
var queue = require('queue-async');
var zlib = require('zlib');
var path = require('path');
var os = require('os');
var fs = require('fs');
var https = require('https');

var backfill = require('../s3-backfill');
var snapshot = require('../s3-snapshot');

var bucket = 'mapbox';
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

        s3Client.send(new ListObjectsCommand({
            Bucket: bucket,
            Prefix: prefix
        })).then(function(data) {
            checkS3(data.Contents.map(function(item) {
                return item.Key;
            }));
        }).catch(function(err) {
            throw err;
        });
    });

    function checkS3(keys) {
        assert.equal(keys.length, 50, 'all records written to S3');
        var q = queue(20);

        records.forEach(function(expected) {
            var key = crypto.createHash('sha256')
                .update(Dyno.serialize({ id: expected.id }))
                .digest('hex');

            key = [prefix, dynamodb.tableName, key].join('/');
            expected = Dyno.serialize(expected);

            assert.ok(keys.indexOf(key) > -1, 'expected item written for ' + key);
            q.defer(function(next) {
                s3Client.send(new GetObjectCommand({
                    Bucket: bucket,
                    Key: key
                })).then(function(data) {
                    assert.equal(Buffer.from(data.Body).toString(), expected, 'expected data for ' + key);
                    next();
                }).catch(function(err) {
                    throw err;
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

        // Get a signed URL for the S3 object
        getSignedUrl(s3Client, new GetObjectCommand({
            Bucket: bucket,
            Key: snapshotKey
        }), { expiresIn: 3600 }).then(function(url) {
            // Use https to get the object and pipe it to gunzip
            https.get(url, function(response) {
                response.pipe(gunzip);
            });
        }).catch(function(err) {
            throw err;
        });
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
