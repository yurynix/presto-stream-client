const http = require('http');
const https = require('https');
const { Readable } = require('stream');
const { URL } = require('url');
const { Headers } = require('./headers');
const s_objectMode = Symbol('Object Mode');
const s_EOF = Symbol('End of File');
const s_SOF = Symbol('Start of File');
const s_id = Symbol("Query ID");

const QUERY_STATE_CHECK_INTERVAL = 3000; // in ms
class prestoError extends Error {
    constructor(message,props) {
        super(message);
        if(props.hasOwnProperty('data')) {
            this.data = props.data;
        }
        if(props.hasOwnProperty('response_code')) {
            this.response_code = props.response_code;
        }
    }
}

function convertToCSV(input){
    switch (typeof input){
        case 'number':
            return input;
        case 'object':
            if(input === null){
                return `""`;
            }
            //fallthrough
        default:
            return `"${String(input).replace('"','""')}"`;
    }
}

class Statement extends Readable {
    /**
     * @description constructor for Statement class, used to manage a single execution of a Presto query.
     * @param {Object} options - optional parameters highWaterMark, objectMode. (other Readable options are fixed)
     * @param {Client} client - related presto client
     * @param {String} initialUri - the first uri to fetch data from (provided by client)
     * @param {String} queryid - the query ID
     * @param {Boolean} fetchInfo - whether to retrieve Info on success event or not
     * @param {Number} pollInterval - milliseconds to poll for state changes
     */
    constructor(options,client,initialUri,queryId,fetchInfo = false,pollInterval = QUERY_STATE_CHECK_INTERVAL){
        const opts = {objectMode:true,autoDestroy:true,encoding:'utf8'};
        if(options.highWaterMark) {
            opts.highWaterMark = options.highWaterMark;
        }
        super(opts);
        this[s_objectMode] = opts.objectMode; //cannot use this before super
        this[s_EOF] = false;
        this[s_SOF] = false;
        this.client = client;
        this.currentUri = initialUri;
        this.currentState = null;
        this.fetchInfo = (fetchInfo) ? true : false;
        this.columns = null;
        this[s_id] = queryId;
    }
    /**
     * @description returns the query ID related to this statement
     */
    get query_id() {
        return this[s_id];
    }
    /**
     * @description cancel the running query.
     */
    async cancel() {
        this.cancelled = true;
        const { response_code , data } = await this.client.request({ method: 'DELETE', path: this.currentUri });
        if(response_code !== 204) {
            throw new prestoError("query fetch canceled, but Presto query cancel may have failed",{ response_code });
        }
        this.currentUri = null;
        return data;
    }
    /**
     * @description Internal, required as a Readable implementation
     */
    _read() {
        this.run();
    }
    /**
     * @description Internal, required as a Readable implementation
     */
    _destroy(error,callback) {
        if(!['FINISHED', 'CANCELED', 'FAILED'].includes(this.currentState)) {
            //cancel query if it is still running on server
            this.cancel().then(()=>{
                return callback(null);
            },(err)=>{
                return callback(err);
            });
        }
        this.currentUri = null;
        this.client = null;
    }

    /**
     * @description. Internal. fetches packets of data from Presto
     */
    async _run() {
        try {
            const { response_code , data } = await this.client.request(this.currentUri);

            if(this.cancelled) { //check if cancelled before doing anything else.
                if(!this[s_EOF]) { //if not yet reached EOF push null to signal EOF
                    this[s_EOF] = true;
                    this.push(null);
                }
                return; //end run if cancelled
            }
            /*
            * 1st time
                {
                "stats": {
                "rootStage": {
                    "subStages": [
                    {
                        "subStages": [],
                        "processedBytes": 83103149, "processedRows": 2532704,
                        "wallTimeMillis": 20502, "cpuTimeMillis": 3431, "userTimeMillis": 3210,
                        "stageId": "1", "state": "FINISHED", "done": true,
                        "nodes": 3,
                        "totalSplits": 420, "queuedSplits": 0, "runningSplits": 0, "completedSplits": 420
                    }
                    ],
                    // same as substage
                },
                // same as substage
                "state": "RUNNING",
                },
                "data": [ [ 1266352 ] ],
                "columns": [ { "type": "bigint", "name": "cnt" } ],
                "nextUri": "http://localhost:8080/v1/statement/20140120_032523_00000_32v8g/2",
                "partialCancelUri": "http://10.0.0.0:8080/v1/stage/20140120_032523_00000_32v8g.0",
                "infoUri": "http://localhost:8080/v1/query/20140120_032523_00000_32v8g",
                "id": "20140120_032523_00000_32v8g"
                }
            * 2nd time
                    {
                    "stats": {
                    // ....
                    "state": "FINISHED",
                    },
                    "columns": [ { "type": "bigint", "name": "cnt" } ],
                    "infoUri": "http://localhost:8080/v1/query/20140120_032523_00000_32v8g",
                    "id": "20140120_032523_00000_32v8g"
                    }
            */
            if(data.error) {
                if (data.error.message) {
                    this.emit('error',new prestoError(data.error.message,{ response_code , data }));
                } else {
                    this.emit('error',new prestoError('attempt to retrieve next dataUri failed',{ response_code, data }));
                }
                return this.cancel(); //end run if error, and attempt to cancel on Presto as well
            }

            if(this.listenerCount('state') > 0 && !this[s_SOF]) {
                this.emit('state',data.id,data.stats);
            }
            if(this.currentState !== data.stats.state) {
                this.emit('state_change',data.id,data.stats);
                this.currentState = data.stats.state;
                if(this.currentState === 'FINISHED') {
                    this[s_SOF] = true;
                }
            }
            /* presto-main/src/main/java/com/facebook/presto/execution/QueryState.java
            * QUEUED, PLANNING, STARTING, RUNNING, FINISHED, CANCELED, FAILED
            */
            if ((data.stats.state === 'QUEUED' || data.stats.state === 'PLANNING'
             || data.stats.state === 'STARTING' || data.stats.state === 'RUNNING')
            && !data.data) {
                this.currentUri = data.nextUri;
                //currently waiting for the query to finish. wait pollInterval frequency and run again.
                return setTimeout(()=>{ this._run(); },this.pollInterval);
            }
            let canPush = true; //keep track whether downstream can receive data
            if(!this.columns && data.columns) {
                this.columns = data.columns;
                this.emit('columns',data.columns);
                if(this[s_objectMode]) { //object mode, guarantee uniqueness of column names
                    const uniqueColumns = new Set(); const duplicates = {};
                    this.columns.forEach((column)=>{
                        if(uniqueColumns.has(column.name)) {
                            duplicates[column.name] = 1;
                        } else {
                            uniqueColumns.add(column.name);
                        }
                    });
                    if(Object.keys(duplicates).length > 0) { //only run if duplicates detected
                        for (let i = 0; i < this.columns.length; i++) {
                            const colName = this.columns[i].name;
                            if(duplicates.hasOwnProperty(colName)) {
                                this.columns[i].name += '_' + duplicates[colName];
                                duplicates[colName]++;
                            }
                        }
                    }
                } else { //not object mode, push out CSV header
                    canPush = this.push(this.columns.map(i=>`"${String(i.name).replace('"','""')}"`).join(',') + '\n');
                }
            }
            if (data.data) {
                for (let i = 0; i < data.data.length; i++) { //push data received in this request one by one.
                    //since data is received in bulk it has to be buffered in memory one way or another, so push it all
                    // however keep track whether canPush more, to either stop before next request or keep going immediately
                    if(this[s_objectMode]) { //if object mode, push out object in form column name: value
                        const output = {};
                        for(let y = 0; y < this.columns.length; y++) {
                            output[this.columns[y].name] = data.data[i][y];
                        }
                        canPush = this.push(output);
                    } else {
                        canPush = this.push(data.data[i].map(convertToCSV).join(',') + '\n');
                    }
                }

            }
            if(data.nextUri) {
                this.currentUri = data.nextUri;
                if(canPush) { //if not too much pressure on writeStream, get the next value without waiting.
                    return this._run();
                }
            } else {
                //if no nextUri, reached end of datastream.
                // if not already done (from cancellation) push null to signal EOF.
                if(!this[s_EOF]) {
                    this[s_EOF] = true;
                    this.push(null);
                }
                if(this.fetchInfo && data.infoUri) {
                    const { data : info } = await this.client.request(data.infoUri);
                    this.emit('success',{ stats: data.stats , info });
                } else {
                    this.emit('success',data.stats);
                }
            }
        } catch(error) {
            this.emit('error',error);
        }
    }
}
class Client {
    /**
     * @description constructor for class Client, a presto client.
     * @param {Object} args - host,port,user,password, catalog, schema, source, pollInterval, jsonParser , objectMode , ssl
     */
    constructor(args){
        if (!args) {args = {};}
        // exports.version set in index.js of project root
        this.userAgent = 'presto-stream-client ' + exports.version;
        this.host = args.host || 'localhost';
        this.port = args.port || 8080;
        this.user = args.user || process.env.USER;
        this.password = args.password || null;
        this.catalog = args.catalog;
        this.schema = args.schema || 'default';
        this.source = args.source || 'nodejs-client';
        this.pollInterval = args.pollInterval || QUERY_STATE_CHECK_INTERVAL;
        this.jsonParser = args.jsonParser || JSON;
        if(args.hasOwnProperty('objectMode')){
            this.objectMode = (args.objectMode) ? true : false;
        } else {
            this.objectMode = false;
        }
        if (args.ssl && typeof args.ssl === 'object') {
            this.ssl = args.ssl;
        } else {
            this.ssl = false;
        }
    }

    request(opts) {
        let contentBody = null;
        if (opts instanceof Object) {
            opts.host = this.host;
            opts.port = this.port;
            opts = Object.assign({}, opts,{headers:{}},this.ssl);
            opts.headers[Headers.USER_AGENT] = this.userAgent;
            if (this.user) {opts.headers[Headers.USER] = this.user;}
            if (this.source) {opts.headers[Headers.SOURCE] = this.source;}
            /**
             * Apply an Authorization header if the user
             * has specified a password.
             */
            if (this.password){
                opts.headers[Headers.AUTHORIZATION] = 'Basic ' + new Buffer(this.user + ":" + this.password).toString("base64");
            }
            if (opts.body) {contentBody = opts.body;}
            opts.agent = (this.ssl) ? new https.Agent(opts) : new http.Agent(opts);
        } else if (this.password){
            /**
             * The request has not come through as an object,
             * it is a nextUri string. Hence, if basic auth is to be applied
             * to the request, we must parse the
             * incoming string (opts) and form a new request object with
             * appropriate basic auth headers
             *
             * Otherwise, if basic auth not required, continue
             * with request as normal leaving the opts string
             * unmodified for the below adapter.request() call
            */
            const href = new URL(opts);
            opts = {};
            opts.host = href.hostname;
            opts.port = href.port;
            opts.path = href.pathname;
            opts.headers = {};
            opts.headers[Headers.AUTHORIZATION] = 'Basic ' + new Buffer(this.user + ":" + this.password).toString("base64");
        }
        return new Promise((resolve,reject)=>{
            const req = (this.ssl) ? https.request(opts) : http.request(opts);
            //buffer the entire response before handing off to next step (required since response will be JSON)
            req.on('response',(res)=>{
                const response_code = res.statusCode;
                const response_data = [];
                res.on('data', (chunk)=>{
                    response_data.push(chunk);
                }).on('end', ()=>{
                    let data = Buffer.from(response_data).toString('utf8');
                    if(response_code < 300 && data[0] === '{' || data[0] === '[') {
                        try {
                            data = this.jsonParser.parse(data);
                        } catch (x) {
                            /* ignore json parse error (and don't parse) for non-json content body */
                        }
                    }
                    return resolve({ response_code, data });
                });
            }).on('error', function(e){
                return reject(e);
            });
            if (contentBody) {req.write(contentBody);}
            req.end();
        });
    }

    async nodes() { // TODO: "failed" nodes not supported yet
        const { response_code , data } = await this.request({ method: 'GET', path: '/v1/node' });

        if(response_code !== 200) {
            throw new prestoError("node list api returns error" + (data && data.length > 0 ? ":" + data : ""),{ response_code });
        }
        return data;
    }

    async status(query_id) {
        const { response_code , data } = await this.request({ method: 'GET', path: '/v1/query/' + query_id });
        if(response_code !== 200) {
            throw new prestoError("status info api returns error" + (data && data.length > 0 ? ":" + data : ""),{ response_code });
        }
        return data;
    }

    async kill(query_id) {
        const { response_code , data } = await this.request({ method: 'DELETE', path: '/v1/query/' + query_id });
        if(response_code !== 204) {
            throw new prestoError("query kill api returns error" + (data && data.length > 0 ? ":" + data : ""),{ response_code });
        }
        return null;
    }

    async execute(opts){
        if (!opts.catalog && !this.catalog) {
            throw new Error("catalog not specified");
        } else if (!opts.schema && !this.schema) {
            throw new Error("schema not specified");
        }
        const header = {};
        header[Headers.CATALOG] = opts.catalog || this.catalog;
        header[Headers.SCHEMA] = opts.schema || this.schema;
        if (opts.session) {header[Headers.SESSION] = opts.session;}
        if (opts.timezone) {header[Headers.TIME_ZONE] = opts.timezone;}

        const { response_code , data } = await this.request({ method: 'POST', path: '/v1/statement', headers: header, body: opts.query });

        if(response_code !== 200 || data && data.error) {
            if (data.error.message) {
                throw new prestoError(data.error.message,{ response_code });
            } else {
                throw new prestoError("execution error" + (data && data.length > 0 ? ":" + data : ""),{ response_code });
            }
        } else if (!data.id) {
            throw new prestoError("query id missing in response for POST /v1/statement", { data });
        } else if (!data.nextUri){
            throw new prestoError("nextUri missing in response for POST /v1/statement", { data });
        } else if (!data.infoUri) {
            throw new prestoError("infoUri missing in response for POST /v1/statement", { data });
        }

        /*
    var data = {
      "stats": {
        "processedBytes": 0, "processedRows": 0,
        "wallTimeMillis": 0, "cpuTimeMillis": 0, "userTimeMillis": 0,
        "state": "QUEUED",
        "scheduled": false,
        "nodes": 0,
        "totalSplits": 0, "queuedSplits": 0, "runningSplits": 0, "completedSplits": 0,
      },
      "nextUri": "http://localhost:8080/v1/statement/20140120_032523_00000_32v8g/1",
      "infoUri": "http://localhost:8080/v1/query/20140120_032523_00000_32v8g",
      "id": "20140120_032523_00000_32v8g"
    };
     */
        return new Statement({},this,data.nextUri,data.id,opts.info || false,this.pollInterval);
    }
}

module.exports = { Client , Statement };
