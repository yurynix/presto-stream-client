const http = require('http');
const https = require('https');
const { Readable } = require('stream');
const { URL } = require('url');
const { Headers } = require('./headers');
const s_objectMode = Symbol('Object Mode');
const s_EOF = Symbol('End of File');

const QUERY_STATE_CHECK_INTERVAL = 3000; // in ms

function adapterFor(protocol) {
    switch (protocol) {
        case 'http:':
            return http;
        case 'https:':
            return https;
        default:
            throw new Error('invalid protocol');
    }
}

class prestoError extends Error {
    constructor(message,props) {
        super(message);
        for (const prop of props) {
            this[prop] = props;
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
     * @param {Object} options - optional parameters highWaterMark, enconding, objectMode. (other Readable options are fixed)
     * @param {Client} client - related presto client
     * @param {String} initialUri - the first uri to fetch data from (provided by client)
     * @param {Boolean} fetchInfo - whether to retrieve Info on success event or not
     * @param {Number} pollInterval - milliseconds to poll for state changes
     */
    constructor(options,client,initialUri,fetchInfo = false,pollInterval = QUERY_STATE_CHECK_INTERVAL){
        const opts = {objectMode:true,autoDestroy:true};
        if(options.highWaterMark) {
            opts.highWaterMark = options.highWaterMark;
        }
        if(options.encoding) {
            opts.encoding = options.encoding;
        }
        if(options.objectMode) {
            opts.objectMode = (options.objectMode) ? true : false;
        }
        super(opts);
        this[s_objectMode] = opts.objectMode; //cannot use this before super
        this[s_EOF] = false;
        this.client = client;
        this.currentUri = initialUri;
        this.currentState = null;
        this.fetchInfo = fetchInfo;
        this.columns = null;
        this.readBuffer = [];
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
        this.readBuffer = [];
    }
    /**
     * @description. Internal. fetches packets of data from Presto
     */
    async run() {
        try {
            //if some results were stored because the previous fetch from Presto passed the writestream high water mark, run through them first
            //only getnew if writestream can handle it.
            if(this.readBuffer.length > 0) {
                let getNew = true;
                while(this.readBuffer.length > 0 && getNew){
                    getNew = this.push(this.readBuffer.shift());
                }
                if(!getNew) {
                    return;
                }
            }
            const { response_code , data } = await this.client.request(this.currentUri);
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
                if(data.error instanceof Error) {
                    this.emit('error',data.error);
                } else if (data.error.message) {
                    this.emit('error',new prestoError(data.error.message,{ response_code , data }));
                } else {
                    this.emit('error',new prestoError('attempt to retrieve next dataUri failed',{ response_code, data }));
                }
                return this.cancel();
            }

            if(this.cancelled) {
                if(!this[s_EOF]) { //if not yet reached EOF push null to signal EOF
                    this[s_EOF] = true;
                    this.push(null);
                }
                return; //end run if cancelled
            }

            let canPush = true;
            if(this.listenerCount('state') > 0) {
                this.emit('state',data.id,data.stats);
            }
            if(this.currentState !== data.stats.state) {
                this.emit('state_change',data.id,data.stats);
                this.currentState = data.stats.state;
            }
            if(data.columns && !this.columns) {
                this.columns = data.columns;
                this.emit('columns',data.columns);
                if(this[s_objectMode]) {
                    canPush = this.push(this.columns.map(i => i.name));
                } else { //csv format
                    canPush = this.push(this.columns.map(i=>`"${String(i.name).replace('"','""')}"`).join(',') + '\n');
                }
            }
            /* presto-main/src/main/java/com/facebook/presto/execution/QueryState.java
            * QUEUED, PLANNING, STARTING, RUNNING, FINISHED, CANCELED, FAILED
            */
            let rowRead = 0;
            if (data.stats.state === 'QUEUED'
                    || data.stats.state === 'PLANNING'
                    || data.stats.state === 'STARTING'
                    || data.stats.state === 'RUNNING' && !data.data) {
                this.currentUri = data.nextUri;
                //currently waiting for the query to finish. wait pollInterval frequency and run again.
                await new Promise((resolve)=>{
                    setTimeout(()=>{
                        resolve();
                    },this.pollInterval);
                });
                return this.run();
            } else if (data.data) {
                while (rowRead < data.data.length && canPush) {
                    if(this[s_objectMode]) {
                        canPush = this.push(data.data[rowRead]);
                    } else {
                        canPush = this.push(data.data[rowRead].map(convertToCSV).join(',') + '\n');
                    }
                    rowRead++;
                }
                while (rowRead < data.data.length) { //canPush stopped the previous loop prematurely
                    //store any unread values to read from memory next run through
                    if(this[s_objectMode]) {
                        this.readBuffer.push(data.data[rowRead]);
                    } else {
                        this.readBuffer.push(data.data[rowRead].map(convertToCSV).join(',') + '\n');
                    }
                    rowRead++;
                }

            }

            if(data.nextUri && canPush) {
                //if not too much pressure on writeStream, get the next value without waiting.
                this.currentUri = data.nextUri;
                return this.run();
            } else if (canPush) {
                //if no nextUri, reached end of datastream, if not already done (from cancellation) push null to signal EOF.
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
     * @param {Object} args - host,port,user,password, catalog, schema, source, pollInterval, jsonParser , ssl
     */
    constructor(args){
        if (!args) {args = {};}
        // exports.version set in index.js of project root
        this.userAgent = 'presto-client-stream ' + exports.version;
        this.host = args.host || 'localhost';
        this.port = args.port || 8080;
        this.user = args.user || process.env.USER;
        this.password = args.password || null;
        this.protocol = 'http:';
        this.catalog = args.catalog;
        this.schema = args.schema;
        this.source = args.source || 'nodejs-client';
        this.pollInterval = args.pollInterval || QUERY_STATE_CHECK_INTERVAL;
        this.jsonParser = args.jsonParser || JSON;
        if (args.ssl) {
            this.protocol = 'https:';
            this.ssl = args.ssl;
        }
    }

    request(opts) {
        const adapter = adapterFor(this.protocol);
        const parser = this.jsonParser;
        let contentBody = null;
        if (opts instanceof Object) {
            opts.host = this.host;
            opts.port = this.port;
            opts.protocol = this.protocol;
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
            opts.agent = new adapter.Agent(opts); // Otherwise SSL params are ignored.
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
            opts.protocol = this.protocol;
            opts.path = href.pathname;
            opts.headers = {};
            opts.headers[Headers.AUTHORIZATION] = 'Basic ' + new Buffer(this.user + ":" + this.password).toString("base64");
        }
        return new Promise((resolve,reject)=>{
            const req = adapter.request(opts);
            req.on('response',(res)=>{
                const response_code = res.statusCode;
                const response_data = [];
                res.setEncoding('utf8');
                res.on('data', (chunk)=>{
                    response_data.push(chunk);
                }).on('end', ()=>{
                    let data = response_data.join('');
                    if (response_code < 300 && (data[0] === '{' || data[0] === '[')) {
                        try { data = parser.parse(data); } catch (x) {
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

    async nodes(opts) { // TODO: "failed" nodes not supported yet
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
        } else if (!opts.success && !opts.callback) {
            throw new Error("callback function 'success' (or 'callback') not specified");
        }
        const header = {};
        header[Headers.CATALOG] = opts.catalog || this.catalog;
        header[Headers.SCHEMA] = opts.schema || this.schema;
        if (opts.session) {header[Headers.SESSION] = opts.session;}
        if (opts.timezone) {header[Headers.TIME_ZONE] = opts.timezone;}

        const { response_code , data } = await this.request({ method: 'POST', path: '/v1/statement', headers: header, body: opts.query });

        if(response_code !== 200 || data && data.error) {
            if(data.error instanceof Error){
                data.error.response_code = response_code;
                throw data.error;
            } else if (data.error.message) {
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
        return new Statement({},this,data.nextUri,opts.info || false,this.pollInterval);
    }
}

module.exports = { Client , Statement };
