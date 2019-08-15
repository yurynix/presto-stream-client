exports = {
    clients:[
        {user:'user'},
        {user:'user',
            password:'password',
            host:'localhost',
            port:8100
        },
        {user:'user',
            host:'localhost',
            port:443,
            ssl:{}
        },
        {user:'user',
            host:'localhost',
            password:'password',
            port:8443,
            ssl:{}
        }
    ],
    testQueries:[
        'SHOW SCHEMAS',
        'SHOW SCHEMAS',
        'SELECT * FROM test.example LIMIT 5',
        'SELECT * FROM test.example LIMT 10'
    ]
};
