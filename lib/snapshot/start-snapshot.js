var crypto = require('crypto');

module.exports = startSnapshot;

/**
 * Generates a set of prefixes. Each map job will scan one of these.
 *
 * @param {string} message - the incoming message containing the base s3 url
 * @returns {object} description of the job to be completed, including all the
 * messages for map processes
 */
function startSnapshot(message) {
  message = JSON.parse(message);

  var id = crypto.randomBytes(16).toString('hex');
  var total = 16;
  var parts = [];

  for (var part = 0; part < total; part++)
    parts.push({ id, part: part + 1, table: message.table, url: message.url, prefix: part.toString(16) });

  return { id, parts, total };
}
