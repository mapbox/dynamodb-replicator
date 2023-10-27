var _ = require('underscore');
var util = require('util');

module.exports = function(category, level, template) {
    category = category || 'default';
    template = template || '[${timestamp}] [${level}] [${category}]';
    level = level || process.env.FASTLOG_LEVEL || 'info';
    var levels = ['debug', 'info', 'warn', 'error', 'fatal'];
    return _(levels).reduce(function(logger, l) {
        logger[l] = function() {
            if (levels.indexOf(l) < levels.indexOf(level)) return;
            var prefix = template
                .replace(/\${ ?timestamp ?}/g, new Date().toUTCString())
                .replace(/\${ ?level ?}/g, l)
                .replace(/\${ ?category ?}/g, category);
            var msg;
            if (arguments[0] instanceof Error) {
                var err = arguments[0];
                // Error objects passed directly.
                var lines = [err.toString()];
                if (err.stack) {
                    var stack = err.stack.split('\n');
                    lines = lines.concat(stack.slice(1, stack.length));
                }
                _(err).each(function(val, key) {
                    if (_(val).isString() || _(val).isNumber()) lines.push('    ' + key + ': ' + val);
                });

                msg = lines.join('\n');
            } else {
                // Normal string messages.
                msg = util.format.apply(this, arguments);
            }

            console.log('%s %s', prefix, msg);
            return util.format('%s %s', prefix, msg);
        };
        return logger;
    }, {});
};