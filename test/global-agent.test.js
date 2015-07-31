var https = require('https');
var test = require('tape');
require('..');

test('https globalAgent', function(t) {
    t.equal(https.globalAgent.maxSockets, 16,
      'https.globalAgent.maxSockets shoud equal 16. It equals ' + https.globalAgent.maxSockets);
    t.end();
});
