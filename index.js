const lib = require('./lib/index');
lib.version = require('./package.json').version;

exports = {Client:lib.Client};
