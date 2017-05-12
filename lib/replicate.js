var Dyno = require('@mapbox/dyno');

module.exports = function replicate(event, context, callback) {
  var replicaConfig = {
    accessKeyId: process.env.ReplicaAccessKeyId || undefined,
    secretAccessKey: process.env.ReplicaSecretAccessKey || undefined,
    table: process.env.ReplicaTable,
    region: process.env.ReplicaRegion,
    maxRetries: 1000,
    httpOptions: {
      timeout: 2000,
      agent: module.exports.agent
    }
  };

  if (process.env.ReplicaEndpoint) replicaConfig.endpoint = process.env.ReplicaEndpoint;
  var replica = new Dyno(replicaConfig);

  var keyAttrs = Object.keys(event.Records[0].dynamodb.Keys);

  var filterer;
  if (process.env.TurnoverRole && process.env.TurnoverAt) {
    // Filterer function should return true if the record SHOULD be processed
    filterer = function(record) {
      var created = Number(record.dynamodb.ApproximateCreationDateTime + '000');
      var turnoverAt = Number(process.env.TurnoverAt);
      if (process.env.TurnoverRole === 'BEFORE') return created < turnoverAt;
      else if (process.env.TurnoverRole === 'AFTER') return created >= turnoverAt;
      else return true;
    };
  }

  var count = 0;
  var allRecords = event.Records.reduce(function(allRecords, change) {
    if (filterer && !filterer(change)) return allRecords;
    var id = JSON.stringify(change.dynamodb.Keys);
    allRecords[id] = allRecords[id] || [];
    allRecords[id].push(change);
    count++;
    return allRecords;
  }, {});

  if (count === 0) {
    console.log('No records replicated');
    return callback();
  }

  var params = { RequestItems: {} };
  params.RequestItems[process.env.ReplicaTable] = Object.keys(allRecords).map(function(key) {
    var change = allRecords[key].pop();
    if (change.eventName === 'INSERT' || change.eventName === 'MODIFY') {
      return {
        PutRequest: { Item: Dyno.deserialize(JSON.stringify(change.dynamodb.NewImage)) }
      };
    } else if (change.eventName === 'REMOVE') {
      return {
        DeleteRequest: { Key: Dyno.deserialize(JSON.stringify(change.dynamodb.Keys)) }
      };
    }
  });

  (function batchWrite(requestSet, attempts) {
    requestSet.forEach(function(req) {
      if (req) req.on('retry', function(res) {
        if (!res.error || !res.httpResponse || !res.httpResponse.headers) return;
        if (res.error.name === 'TimeoutError') res.error.retryable = true;
        console.log(
          '[failed-request] %s | request-id: %s | crc32: %s | items: %j',
          res.error.message,
          res.httpResponse.headers['x-amzn-requestid'],
          res.httpResponse.headers['x-amz-crc32'],
          req.params.RequestItems[process.env.ReplicaTable].map(function(req) {
            if (req.DeleteRequest) return req.DeleteRequest.Key;
            if (req.PutRequest) return keyAttrs.reduce(function(key, k) {
              key[k] = req.PutRequest.Item[k];
              return key;
            }, {});
          })
        );
      });
    });

    requestSet.sendAll(100, function(errs, responses, unprocessed) {
      attempts++;

      if (errs) {
        var messages = errs
          .filter(function(err) { return !!err; })
          .map(function(err) { return err.message; })
          .join(' | ');
        console.log('[error] %s', messages);
        return callback(errs);
      }

      if (unprocessed) {
        console.log('[retry] attempt %s contained unprocessed items', attempts);
        return setTimeout(batchWrite, Math.pow(2, attempts), unprocessed, attempts);
      }

      console.log('Replicated ' + count + ' records');
      callback();
    });
  })(replica.batchWriteItemRequests(params), 0);
};
