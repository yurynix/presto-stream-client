const fs = require('fs');
const path = require('path');
const { version } = require(path.resolve(__dirname,'../package.json'));
const index = fs.readFileSync(path.resolve(__dirname,'../lib/index.js'),'utf8');
fs.writeFileSync(path.resolve(__dirname,'../lib/index.js'),index.replace(`require('../package.json').version`,`"${version}"`));
