var https = require('https');
var test = require('tape');
require('..');

test('https globalAgent', function(t) {
    var concurrency = Math.ceil(require('os').cpus().length * 16);
    t.equal(https.globalAgent.maxSockets, concurrency,
      'https.globalAgent.maxSockets should equal ' + concurrency + '. It equals ' + https.globalAgent.maxSockets);
    t.end();
});
