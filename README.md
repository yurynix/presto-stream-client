# presto-stream-client

Node.js streaming,  ES6 Promise-based client library for distributed query engine "Presto".
Forked from [presto-client](https://github.com/tagomoris/presto-client-node)

The client will return either a CSV-formatted stream or, if in object mode, a stream of objects representing rows.
```js
const fs = require('fs');
const util = require('util');
const pipeline = util.promisify(require('stream').pipeline);
const { Client } = require('presto-stream-client');
const client = new Client({user: 'myname'});
(async ()=>{
    const statement = await client.execute({query:'SELECT count(*) as cnt FROM tblname WHERE ...'});
    const writeStream = fs.createWriteStream('/path/to/file.csv');
    statement.on('state_change',(currentState,stats)=>{
        console.log(`state changed to ${currentState} for query ${statement.query_id}`);
        console.log(stats);
    });
    await pipeline(statement,writeStream);
})();


```
Or, in object mode:

```js
client.execute({
    query:   'SELECT count(*) as cnt,usergroup FROM tblname WHERE ...',
    catalog: 'hive',
    schema:  'default',
    objectMode: true
}).then((statement)=>{
    statement.on('columns',(columns)=>{  // [{name:"cnt",type:"bigint"}, {name:"usergroup",type:"varchar"}]
        console.log(columns);
    });
    statement.on('data',(row)=>{
        console.log(row); // {cnt:1234,usergroup:"admin"}
    });
    statement.on('end',()=>{
        console.log('done');
    });
},(error)=>{
    console.error(error);
});
```

# Installation

```
npm install presto-stream-client
```

# Classes

## Client

### Properties

* catalog [string]: the default catalog to assign for statements.
* schema [string]: the default schema to assign for statements.
* ssl [Object OR false]: if false, no SSL. If an Object, ssl set with settings as in ssl object. (read-only)

### Constructor
```js
const client = new Client(opts);
```
* opts [object]
  * host [string]
    * Presto coordinator hostname or address (default: localhost)
  * port [integer]
    * Presto coordinator port (default: 8080)
  * ssl [object] (optional)
    * If provided, will connect via HTTPS instead of HTTP using the provided ssl settings. (pass an empty object if it is desired to connect via SSL with no special settings)
    * Settings are defined as per [Node.js core https module](https://nodejs.org/dist/latest-v10.x/docs/api/https.html#https_https_request_options_callback).
  * user [string]
    * Username of query (default: process user name)
  * password [string] (optional)
    * If provided, will add Basic Authorization headers containing user and password to all requests.
  * source [string]
    * Source of query (default: nodejs-client)
  * catalog [string] (optional)
    * Default catalog name (default: 'hive', may be changed at statement execution)
  * schema [string] (optional)
    * Default schema name (default: 'default', may be changed at statement execution)
  * pollInterval [integer]
    * frequency in milliseconds to poll for state changes *before* data is ready (default: 3000). (After data is ready, it is retrieved as fast as possible)
  * jsonParser [object]
    * Custom json parser if required (default: `JSON`)
  * objectMode [boolean]
    * default objectMode for Statement streams. (default: false, may be changed at statement execution)

### Methods

* **execute(opts)** Execute query on Presto cluster, and return a Promise that resolves to a Readable stream (Statement object). (Using "/v1/statement" HTTP RPC.)
  * opts [object]
    * query [string] the query to run on presto (required)
    * catalog [string] (default: client catalog)
    * schema [string] (default: client schema)
    * timezone [string :optional] the timezone to be passed to presto
    * session [string :optional] the existing presto session string to notify presto this statement belongs to a session
    * objectMode [boolean] whether the statement will run in Object Mode or not. If true, will be a stream of objects. If false, will be a stream of CSV strings.
    * highWaterMark [number] the highWaterMark for the statement stream. (exactly as per stream.Readable)
    * info [boolean :optional] if true, the object returned by the success event will include a property 'info' which contains execution statistics (from infoUri).
* **status(query_id)** Get query current status based on query_id. (Same with 'Raw' of Presto Web in browser.) Returns a Promise that resolves to the status response from Presto or rejects on error.
  * query_id [string] the ID of the query to retrieve status info for.

* **kill(query_id)** Stop a query based on query_id. Returns a Promise that resolves when the query is stopped by Presto or rejects on error.
  * query_id [string] the ID of the query to kill. Note, if there is an existing statement extracting data this may cause errors; in such a case use of `statement.cancel` should be preferred.
* **nodes()** Get node list of the presto cluster. Returns a Promise that resolves to response from presto or rejects on error.
  * failed [boolean] - whether to retrieve currently failing nodes only, or all known nodes. Default false (i.e. extract all known nodes)
* **cluster()** Get cluster statistics. Returns a Promise that resolves to response from presto or rejects on error.

## Statement

Statements extend [stream.Readable](https://nodejs.org/api/stream.html#stream_readable_streams) with the below additional methods & events.
In general, statements should only be created within the execute statment of Client.

### Properties

In addition to standard stream.Readable properties, Statement includes the below property:

* query_id [string, read-only]: the query_id of the current statement.
* client [Client]: reference to the Client object that created this statement
* state [string, read-only]: the current state of this statement (as perceived by the client)
* session [string || null, read-only]: the current session string for this statement. Once the statement has ended, may be used as input in `client.execute` to cause the next statement to execute in the same session. null if query ran with no session or after resolution of `RESET SESSION`
* columns[Array of Objects]: the list of columns of the query. null if columns have not yet been resolved. Intended to be read-only but not restricted.
* fetchInfo [boolean]: Whether to make a final call to infoUri on completion and pass the results to the success event. Can be changed prior to query completion.

### Constructor

Not intended to be called directly. Use `client.execute(options)` to generate a new statement.

### Methods

In addition to standard stream.Readable methods, Statement includes the below methods:
* **cancel()** stops retrieving the result set, ends the stream, and attempts to cancel the query in Presto. Returns a promise that resolves if successful and rejects if error.

### Events

As Statement extends `stream.Readable` it is an event emitter. In addition to the standard Readable events it will additionally emit the below events:

* state: fires every pollInterval from query start until query is FINISHED, CANCELED or FAILED (not guaranteed to fire if CANCELED or FAILED)
    * currentState [string] - the name of the new state
    * stats [object] - running query stats
* state_change: fires every time the query changes state e.g. once on QUEUED, PLANNING, STARTING, RUNNING, FINISHED, or CANCELED, FAILED
    * currentState [string] - the name of the new state
    * stats [object] - running query stats
* columns: fires once, the first time the columns are provided in a response from Presto.
    * columns: array of field info
        * `[ { name: "username", type: "varchar" }, { name: "cnt", type: "bigint" } ]`
* success: fires once on successful completion of data retrieval (data may or may not be completed processing by the stream, so this should not be considered a replacement for the Readable Stream 'end' event). Contains extra information from presto.
    * data [object[] containing 'stats' property with stats. If `statement.fetchInfo` is true will also contain an 'info' property with information from infoUri.

Notes on inherited stream.Readable Events:
* data: If objectMode is true, data will be fired per row and will be an object in format: `{'column_name':'value'}`. If objectMode is false, data will be successive strings each representing a row in CSV file format with the first row being the column names.
* end: As per standard stream.Readable, this event will fire when the readable stream is completed.
* error: As per standard stream.Readable, this event will fire if there is an error. Please note: Stream error events by default crash node if not handled.
  * Errors may be standard Nodejs errors or may be a `prestoError`. prestoErrors may have additional properties response_code, data and response_type which are extracted directly from the response from the presto server. (prestoErrors have the `name` property of `prestoError` )

## BIGINT value handling

Javascript standard `JSON` module cannot handle BIGINT values correctly due to floating point precision problems.

```js
JSON.parse('{"bigint":1139779449103133602}').bigint //=> 1139779449103133600
```

If your query puts numeric values in its results and precision is important for that query, you can swap JSON parser with any modules which has `parse` method.

```js
const JSONbig = require('json-bigint');
JSONbig.parse('{"bigint":1139779449103133602}').bigint.toString() //=> "1139779449103133602"
// set client option
const client = new Client({
  // ...
  jsonParser: JSONbig,
  // ...
});
```

## Unit Tests

Unit tests are only partially implemented. help extending the tests is appreciated. Tests can be performed by adding appropriate presto server connection settings set in `testconfig.js` (as per provided `test/exampleconfig.js`)

## TODO

* Proper support for Sessions is somewhat experimental and not fully tested.

# Versions
* 1.0.11 Current release - consistently escape JSON as string in CSV to ensure newlines etc. don't break CSV
* 1.0.10 bugfix to better handle JSON fields in CSV mode
* 1.0.9 bugfixes to work around stream.Readable calling _run too much and remove unhandledPromiseRejection warning for error 410 from Presto
* 1.0.8 Minor bugfix for better handling of presto server errors (code 500))
* 1.0.6 Initial release

# Author & License

* serakfalcon (forked from original code by tagomoris)
* License:
  * MIT (see LICENSE)
