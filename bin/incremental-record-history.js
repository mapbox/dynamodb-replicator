#!/usr/bin/env node

var minimist = require('minimist');
var s3urls = require('s3urls');
var crypto = require('crypto');
var AWS = require('aws-sdk');
var s3 = new AWS.S3();
var queue = require('queue-async');
var Dyno = require('dyno');

var args = minimist(process.argv.slice(2));

function usage() {
    console.error('');
    console.error('Usage: incremental-record-history <tableinfo> <s3url> <recordkey>');
    console.error(' - tableinfo: the table where the record lives, specified as `region/tablename`');
    console.error(' - s3url: s3 folder where the incremental backups live. Table name will be appended');
    console.error(' - recordkey: the key for the record specified as a JSON object');
}

if (args.help) {
    usage();
    process.exit(0);
}

var table = args._[0];

if (!table) {
    console.error('Must provide table information');
    usage();
    process.exit(1);
}

table = table.split('/')[1];

var s3url = args._[1];

if (!s3url) {
    console.error('Must provide an s3url');
    usage();
    process.exit(1);
}

s3url = s3urls.fromUrl(s3url);

var key = args._[2];

if (!key) {
    console.error('Must provide a record key');
    usage();
    process.exit(1);
}

// Sort the attributes in the provided key
key = JSON.parse(key);
key = JSON.stringify(Object.keys(key).sort().reduce(function(keyObj, attr) {
    keyObj[attr] = key[attr];
    return keyObj;
}, {}));

// Converts incoming strings in wire or dyno format into dyno format
try {
    var obj = Dyno.deserialize(key);
    for (var k in obj) if (!obj[k]) throw new Error();
    key = obj;

}
catch (err) { key = JSON.parse(key); }

s3url.Key = [
    s3url.Key,
    table,
    crypto.createHash('md5')
        .update(Dyno.serialize(key))
        .digest('hex')
].join('/');

var q = queue(100);
q.awaitAll(function(err, results) {
    if (err) throw err;
    if (!results.length) return;
    results.sort(function(a, b) { return b.date - a.date }).forEach(function(version) {
        console.log('\nModified: %s', version.date.toISOString());
        console.log('----------------------------------');
        console.log((typeof version.data === 'string' ? version.data : JSON.stringify(version.data, null, 2)) + '\n');
    });
});

(function listVersions(nextVersion, nextKey) {
    var params = {
        Bucket: s3url.Bucket,
        Prefix: s3url.Key
    };

    if (nextVersion) params.VersionIdMarker = nextVersion;
    if (nextKey) params.KeyMarker = nextKey;

    s3.listObjectVersions(params, function(err, data) {
        if (err) throw err;

        data.Versions.forEach(function(version) {
            q.defer(function(next) {
                s3.getObject({
                    Bucket: s3url.Bucket,
                    Key: s3url.Key,
                    VersionId: version.VersionId
                }, function(err, data) {
                    if (err && err.name === 'InvalidObjectState') return next(null, {
                        date: new Date(version.LastModified),
                        data: 'Version archived: ' + version.VersionId
                    });
                    if (err) return next(err);
                    next(null, {
                        date: new Date(version.LastModified),
                        data: Dyno.deserialize(data.Body.toString())
                    });
                });
            });
        });

        data.DeleteMarkers.forEach(function(del) {
            q.defer(function(next) {
                next(null, {
                    date: new Date(del.LastModified),
                    data: 'Record was deleted'
                });
            });
        });

        if (data.IsTruncated && data.NextVersionIdMarker)
            return listVersions(data.NextVersionIdMarker, data.NextKeyMarker);
    });
})();
