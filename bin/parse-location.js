// Regex to find IP address and port
// ex.: 127.0.0.1:80 or localhost:80
var regex = new RegExp('(\\b(?:[0-9]{1,3}\\.){3}[0-9]{1,3}\\b:[0-9]+)|(localhost:[0-9]+)', 'i');

module.exports = {
    parse: function (primary, replica){
        if (regex.test(primary[0])){
            primary = {region: 'local', endpoint:'http://' + primary[0], table: primary[1]};
        } else {
            primary = {region: primary[0], table: primary[1]};
        }

        if (regex.test(replica[0])) {
            replica = {region: 'local', endpoint: 'http://' + replica[0], table: replica[1]};
        } else { 
            replica = {region: replica[0], table: replica[1]};
        }
        return [primary, replica]
    }
}