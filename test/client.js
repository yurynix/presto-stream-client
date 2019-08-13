const { Client } = require('../index');
const clients = [null,null,null,null];
describe('can create and use a new client  with no password, no SSL',function(){
    before('can create a new client',function(){
        clients[0] = new Client({});
    });
});

describe('can create a new client with password, no SSL',function(){
    before('can create a new client',function(){
        clients[1] = new Client({password:1234});
    });
});

describe('can create a new client with ssl and a  password',function(){
    before('can create a new client',function(){
        clients[2] = new Client({ssl:{},password:1234});
    });
});

describe('can create a new client with ssl, no password',function(){
    before('can create a new client',function(){
        clients[3] = new Client({});
    });
});
