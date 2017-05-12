module.exports = {
  replicate: require('./lib/replicate'),
  backup: require('./lib/incremental-backup'),
  snapshot: require('./lib/s3-snapshot')
};
