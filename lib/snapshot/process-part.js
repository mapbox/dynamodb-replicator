var replicator = require('@mapbox/dynamodb-replicator');
var watchbot = require('@mapbox/watchbot');
var s3urls = require('s3urls');

module.exports = processPart;

function processPart(message) {
  var src = s3urls.fromUrl(message.url);

  var config = {
    log: function() {
      var args = Array.prototype.slice.call(arguments);
      args[0] = `[map] ${args[0]}`;
      watchbot.log.apply(watchbot, args);
    },
    source: { bucket: src.Bucket, prefix: `${src.Key}/${message.prefix}` },
    destination: { bucket: src.Bucket, key: `snapshots/${message.id}/${message.prefix}` },
    maxRetries: 50
  };

  return new Promise((resolve, reject) => {
    replicator.snapshot(config, (err) => {
      if (err) return reject(err);
      resolve();
    });
  });
}
