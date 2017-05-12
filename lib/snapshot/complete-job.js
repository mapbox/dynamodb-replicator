var AWS = require('aws-sdk');
var zlib = require('zlib');
var stream = require('stream');
var s3urls = require('s3urls');
var s3scan = require('s3scan');
var watchbot = require('@mapbox/watchbot');
var Dyno = require('@mapbox/dyno');
var split = require('binary-split');

module.exports = completeJob;
module.exports._ = { generateSnapshot, sanitize, fork, upload, sendSnapshotMetric, queueNextSnapshot };

function completeJob(message, topic) {
  var cw = new AWS.CloudWatch();
  var sns = new AWS.SNS({ params: { TopicArn: topic } });
  var s3 = new AWS.S3();

  return Promise.resolve()
    .then(() => {
      message = JSON.parse(message);
      var src = s3urls.fromUrl(message.url);
      return generateSnapshot(s3, message.table, `s3://${src.Bucket}/snapshots/${message.id}`);
    })
    .then((size) => sendSnapshotMetric(cw, message.table, size))
    .then(() => queueNextSnapshot(sns, message.table, message.url));
}

function generateSnapshot(s3, table, url) {
  var bucket = s3urls.fromUrl(url).Bucket;
  var rawDst = `db/${table}/${Date.now()}`;
  var sanitizedDst = `sanitized/${table}/${Date.now()}`;

  var scanner = zlib.createGunzip(); // gunzips aggregate data files which will be written into this via startDownloads function
  var splitter = split(); // splits the aggregate data on newlines, so that we have 1 record per chunk
  var raw = zlib.createGzip(); // gzip the raw backup data on its way into the s3.upload
  var sanitizer = sanitize(); // sanitizes individual records
  var sanitized = sanitizer.pipe(zlib.createGzip()); // sanitize and gzip the raw backup data on its way into the s3.upload
  var duplex = fork(raw, sanitizer); // fork the aggregate output into the two pipelines for raw and sanitized data
  var rawUpload = upload(s3, 'raw', { Bucket: bucket, Key: rawDst, Body: raw }); // upload the raw backup
  var sanitizedUpload = upload(s3, 'sanitized', { Bucket: bucket, Key: sanitizedDst, Body: sanitized }); // upload the sanitized backup

  return list(s3, url).then((keys) => new Promise((resolve, reject) => {
    scanner.on('error', (err) => reject(err));
    splitter.on('error', (err) => reject(err));
    sanitizer.on('error', (err) => reject(err));
    raw.on('error', (err) => reject(err));
    sanitized.on('error', (err) => reject(err));
    duplex.on('error', (err) => reject(err));

    scanner.pipe(splitter).pipe(duplex);

    startDownloads(s3, keys, scanner);

    Promise.all([rawUpload.promise(), sanitizedUpload.promise()])
      .then((results) => resolve(results[0]))
      .catch((err) => reject(err));
  }));
}

function sanitize() {
  function sanitizeRecord(record) {
    if (record.collection && record.collection.indexOf('accounts:') === 0) {
      var id = record.collection.split(':')[1];
      record.email = 'devnull+' + id + '@mapbox.com';
      record.password = '_passwordhash_';
      record.authorizations = '_authorizations_';
    }

    return record;
  }

  return new stream.Transform({
    transform: function(record, enc, callback) {
      record = record.toString();
      if (!record) return callback();

      record = Dyno.deserialize(record);
      try { record = sanitizeRecord(record); }
      catch (err) { return callback(err); }

      record = Dyno.serialize(record);
      this.push(`${record}\n`);
      callback();
    }
  });
}

function fork(writeA, writeB) {
  var writeTo = (stream, data) => new Promise((resolve) => {
    if (stream.write(`${data}\n`)) return resolve();
    stream.once('drain', resolve);
  });

  var duplex = new stream.Writable({
    write: function(chunk, enc, callback) {
      Promise.all([writeTo(writeA, chunk), writeTo(writeB, chunk)])
        .then(() => callback());
    }
  });

  duplex.on('finish', () => {
    writeA.end();
    writeB.end();
  });

  writeA.on('error', (err) => duplex.emit('error', err));
  writeB.on('error', (err) => duplex.emit('error', err));

  return duplex;
}

function upload(s3, name, params) {
  let size = 0;
  var req = s3.upload(params);
  req.on('httpUploadProgress', function(details) {
    watchbot.log(`[reduce] [${name}] Starting upload of part #${details.part - 1}, ${size} bytes uploaded`);
    size = details.loaded;
  });

  req.promise = () => new Promise((resolve, reject) => {
    req.send((err) => {
      if (err) return reject(err);
      resolve(size);
    });
  });

  return req;
}

function sendSnapshotMetric(cw, table, size) {
  watchbot.log(`[reduce] report size metrics for ${table} snapshot`);
  return cw.putMetricData({
    Namespace: 'Mapbox',
    MetricData: [
      {
        MetricName: 'BackupSize',
        Dimensions: [{ Name: 'TableName', Value: table }],
        Value: size,
        Unit: 'Bytes'
      }
    ]
  }).promise();
}

function queueNextSnapshot(sns, table, url) {
  watchbot.log(`[reduce] queue next snapshot for ${table}`);
  return sns.publish({
    Subject: 'start-snapshot',
    Message: JSON.stringify({ table, url })
  }).promise();
}

function list(s3, url) {
  watchbot.log(`[reduce] list keys in ${url}`);
  var keys = [];
  var lister = s3scan.List(url, { s3, objectMode: true });
  return new Promise((resolve, reject) => {
    lister.on('error', (err) => reject(err));
    lister.on('data', (key) => keys.push(key));
    lister.on('end', () => {
      watchbot.log(`[reduce] listed keys: ${keys.map((key) => key.Key).join(', ')}`);
      resolve(keys);
    });
  });
}

function startDownloads(s3, keys, output) {
  var key = keys.shift();
  if (!key) return output.end();

  watchbot.log(`[reduce] download ${key.Key} from ${key.Bucket}`);
  s3.getObject(key).createReadStream()
    .on('error', (err) => output.emit('error', err))
    .on('end', () => startDownloads(s3, keys, output))
    .pipe(output, { end: false });
}
