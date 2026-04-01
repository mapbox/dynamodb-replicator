'use strict';

// use a different S3 bucket by defining BackupBucket in the ENV
process.env.BackupBucket = process.env.BackupBucket || 'mapbox';
