var crypto = require('crypto');
var _ = require('underscore');

module.exports = function(num) {
    return _.range(0, num).map(function() {
        return {
            id: crypto.randomBytes(16).toString('hex'),
            arange: Math.random(),
            data: crypto.randomBytes(256).toString('base64')
        };
    });
};
