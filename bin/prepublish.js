const fs = require('fs');
const { version } = require('../package.json');
const readme = fs.readFileSync('../README.md','utf8');
fs.writeFileSync('../README.md',readme.replace('{{version}}',version));
const index = fs.readFileSync('../lib/index.js','utf8');
fs.writeFileSync('../lib/index.js',index.replace(`require('../package.json').version`),`"${version}"`);


