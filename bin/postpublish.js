const exec = require('child_process').exec;
const path = require('path');
exec('git checkout -- README.md',{cwd:path.resolve(__dirname,'..')},function(){
    exec('git checkout -- index.js',{cwd:path.resolve(__dirname,'..','lib')},function(){
        process.close(0);
    });
});
