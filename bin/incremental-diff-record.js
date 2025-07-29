#!/usr/bin/env node

var minimist = require('minimist');
var s3urls = require('s3urls');
var Dyno = require('@mapbox/dyno');
var crypto = require('crypto');
var { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
var assert = require('assert');

var args = minimist(process.argv.slice(2));

function usage() {
    console.error('');
    console.error('Usage: incremental-diff-record <tableinfo> <s3url> <recordkey>');
    console.error(' - tableinfo: the table where the record lives, specified as `region/tablename`');
    console.error(' - s3url: s3 folder where the incremental backups live');
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

var region = table.split('/')[0];
table = table.split('/')[1];

var s3Client = new S3Client({ region: region });

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
    crypto.createHash('sha256')
        .update(Dyno.serialize(key))
        .digest('hex')
].join('/');

var dyno = Dyno({
    region: region,
    table: table
});

dyno.getItem({ Key: key }, function(err, data) {
    if (err) throw err;
    var dynamoRecord = data.Item;

    s3Client.send(new GetObjectCommand(s3url))
        .then(data => {
            var s3data = Dyno.deserialize(data.Body.toString());

            console.log('DynamoDB record');
            console.log('--------------');
            console.log(dynamoRecord);
            console.log('');

            console.log('Incremental backup record (%s)', s3url.Key);
            console.log('--------------');
            console.log(s3data);
            console.log('');

            try {
                assert.deepEqual(s3data, dynamoRecord);
                console.log('----------------------------');
                console.log('✔ The records are equivalent');
                console.log('----------------------------');
            }
            catch (err) {
                console.log('--------------------------------');
                console.log('✘ The records are not equivalent');
                console.log('--------------------------------');
            }
        })
        .catch(err => {
            if (err.$metadata && err.$metadata.httpStatusCode === 404) {
                var s3data = undefined;

                console.log('DynamoDB record');
                console.log('--------------');
                console.log(dynamoRecord);
                console.log('');

                console.log('Incremental backup record (%s)', s3url.Key);
                console.log('--------------');
                console.log(s3data);
                console.log('');

                console.log('--------------------------------');
                console.log('✘ The records are not equivalent');
                console.log('--------------------------------');
            } else {
                throw err;
            }
        });
});
