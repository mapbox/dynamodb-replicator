#!/usr/bin/env node

var minimist = require('minimist');
var Dyno = require('dyno');

var args = minimist(process.argv.slice(2));

function usage() {
    console.error('');
    console.error('Usage: replicate-record <primary tableinfo> <replica tableinfo> <recordkey>');
    console.error(' - primary tableinfo: the primary table to replicate from, specified as `region/tablename`');
    console.error(' - replica tableinfo: the replica table to replicate to, specified as `region/tablename`');
    console.error(' - recordkey: the key for the record specified as a JSON object');
}

if (args.help) {
    usage();
    process.exit(0);
}

var primary = args._[0];

if (!primary) {
    console.error('Must provide primary table information');
    usage();
    process.exit(1);
}

var primaryDyno = Dyno({
    table: primary.split('/')[1],
    region: primary.split('/')[0]
});

var replica = args._[1];

if (!replica) {
    console.error('Must provide replica table information');
    usage();
    process.exit(1);
}

var replicaDyno = Dyno({
    table: replica.split('/')[1],
    region: replica.split('/')[0]
});

var key = args._[2];

if (!key) {
    console.error('Must provide a record key');
    usage();
    process.exit(1);
}

// Converts incoming strings in wire or dyno format into dyno format
try {
    var obj = Dyno.deserialize(key);
    for (var k in obj) if (!obj[k]) throw new Error();
    key = obj;

}
catch (err) { key = JSON.parse(key); }

primaryDyno.getItem({ Key: key, ConsistentRead: true }, function(err, data) {
    if (err) throw err;
    var item = data.Item;

    if (!item) return replicaDyno.deleteItem({ Key: key }, function(err) {
        if (err) throw err;
    });

    replicaDyno.putItem({ Item: item }, function(err) {
        if (err) throw err;
    });
});
