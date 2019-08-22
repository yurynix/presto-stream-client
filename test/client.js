/* eslint-disable no-console */
const fs = require('fs');
const path = require('path');
const { promisify } = require('util');
const { pipeline } = require('stream');
const { Client } = require('../lib/index');
if(!fs.existsSync(path.join(__dirname,'testconfigs.js'))){
    console.error('Error, tests of the presto client have a dependency on presto configuration settings.');
    console.error('You must create the file testconfigs.js which exports the settings specific to your setup (example exampleconfigs.js provided)');
    process.exit(1);
}
const configs = require('./testconfigs.js');
const clients = [null,null,null,null];
const statusQueryIds = [null,null,null,null];
const describes = [
    'client with no password, no SSL',
    'client with password, no SSL',
    'client with no password & SSL',
    'client with password & SSL'
];

for (let i = 0; i < 4; i++) {
    describe(describes[i],function(){
        before('can create a new client',function(){
            clients[i] = new Client(configs.client[i]);
        });
        it('can query nodes',async function(){
            console.log(await clients[i].nodes());
        });
        it('can query cluster',async function(){
            console.log(await clients[i].cluster());
        });
        it('can execute a query in object mode',async function(){
            this.timeout(60000);
            const statement = await clients[i].execute({query:configs.query[i],objectMode:true});
            await new Promise((resolve,reject)=>{
                statement.on('state_change',(state,info)=>{
                    console.log(state);
                    console.log(info);
                }).on('data',(row)=>{
                    console.log(row);
                }).on('columns',(columns)=>{
                    console.log(columns);
                }).on('error',(e)=>{
                    console.error(e);
                    return reject(e);
                }).on('end',()=>{
                    return resolve();
                });
            });
        });

        it('can execute a query in file mode',async function(){
            this.timeout(60000);
            const statement = await clients[i].execute({query:configs.query[i]});
            const writeStream = fs.createWriteStream(path.resolve(__dirname,'test.csv'));
            statement.on('state_change',(state,info)=>{
                console.log(state);
                console.log(info);
            }).on('columns',(columns)=>{
                console.log(columns);
            });
            await promisify(pipeline)(statement,writeStream);
        });

        it('can cancel a query from client.kill',async function(){
            this.timeout(60000);
            const statement = await clients[i].execute({query:configs.query[i]});
            statement.on('state_change',(state,info)=>{
                console.log(state);
            }).on('data',(row)=>{
                console.log(row);
            }).on('error',(e)=>{
                console.error('error occured');
                console.error(e);
            });
            await new Promise((resolve)=>{setTimeout(()=>{resolve();},1000);}); //wait 1 s
            console.log(await clients[i].kill(statement.query_id));

        });

        it('can cancel a query from statement.cancel',async function(){
            this.timeout(60000);
            const statement = await clients[i].execute({query:configs.query[i]});
            statement.on('state_change',(state,info)=>{
                console.log(state);
            }).on('data',(data)=>{
                console.log(data);
            }).on('error',(e)=>{
                console.error('error occured');
                console.error(e);
            });
            await new Promise((resolve)=>{setTimeout(()=>{resolve();},2000);}); //wait 2 s
            console.log(await statement.cancel());
            statusQueryIds[i] = statement.query_id;
        });

        it('can retreive a existing queries status',async function(){
            console.log(await clients[i].status(statusQueryIds[i]));
        });
    });
}
