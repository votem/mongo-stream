const version = require('./package.json').version;
const program = require('commander');
const fs = require('fs');
const logger = new (require('service-logger'))(__filename);

program
  .version(version)
  .option('-c, --config <path>', 'Path to configuration file', './config/default.json')
  .option('--ssl', 'Enables connection to a mongod or mongos that has TLS/SSL support enabled')
  .option('--sslCA <path>', 'Specifies the .pem file that contains the root certificate chain from the Certificate Authority')
  .option('--sslKey <path>', 'Specifies the .pem file that contains both the TLS/SSL key')
  .option('--sslCert <path>', 'Specifies the .pem file that contains both the TLS/SSL certificate')
  .option('--fullUrl <value>', 'Specifies the full url (username, password, URI arguments included) that will be used to connect to the mongoDB server')
  .option('--poolSize <n>', 'Specifies the max number of connections to make to the mongoDB server')
  .option('--db <value>', 'Specifies the database to connect to on the mongoDB server')
  .option('--bulkSize <n>', 'Specifies the size of bulk requests to operate on')
  .option('--adminPort <n>', 'Specifies the port that mongo-stream will listen to')
  .option('--mappings <path>', 'Specifies the path to a file containing mappings from mongo collections to elasticsearch routes')
  .option('-l, --logLevel <value>', 'Specifies the severity level for printing logs (allowed values are in the log levels section of https://www.npmjs.com/package/winston-syslog)')
  .parse(process.argv);

// First parse everything from the specified config file, then replace with command line args
const CONFIG = require(program.config);

// set log level for service-logger
logger.logLevel = program.logLevel || CONFIG.logLevel || 'info';

// build mongo connection url
let url = 'mongodb://';
if (CONFIG.mongo.user) {
  url += encodeURIComponent(CONFIG.mongo.user);
  if (CONFIG.mongo.password) url += `:${encodeURIComponent(CONFIG.mongo.password)}`;
  url += '@';
}
url += CONFIG.mongo.url;

// parse connect options into the url
if (CONFIG.mongo.connectOptions) {
  const options = Object.keys(CONFIG.mongo.connectOptions);
  for (let i = 0; i < options.length; i++) {
    if (i === 0) url += '?';
    else url += '&';
    url += `${options[i]}=${CONFIG.mongo.connectOptions[options[i]]}`;
  }
}

// overwrite values with command line arguments
let mongoOpts = CONFIG.mongo.options;
mongoOpts.poolSize = program.poolSize || mongoOpts.poolSize;
let db = program.db || CONFIG.mongo.database;
let bulkSize = program.bulkSize || CONFIG.bulkSize;
let adminPort = program.adminPort || CONFIG.adminPort;
if (program.ssl) {
  mongoOpts.sslCA = program.sslCA;
  mongoOpts.sslCert = program.sslCert;
  mongoOpts.sslKey = program.sslKey;
}


// finally, replace paths to ssl certs with the values within the specified files
if (mongoOpts.ssl) {
  mongoOpts.sslCA = fs.readFileSync(mongoOpts.sslCA, 'utf8');
  mongoOpts.sslKey = fs.readFileSync(mongoOpts.sslKey, 'utf8');
  mongoOpts.sslCert = fs.readFileSync(mongoOpts.sslCert, 'utf8');
}

//warn user if their collection names are too large to help debug errors in configuration
const COLL_NAME_MAX_LENGTH = 110;
CONFIG.mongo.collections.forEach((collection) => {
  if(collection.length > COLL_NAME_MAX_LENGTH){
    logger.warn(`The collection ${collection} is ${collection.length - COLL_NAME_MAX_LENGTH} characters too large.  Mongo does not allow names to be larger that 120 bytes.(https://docs.mongodb.com/manual/reference/limits/#Restriction-on-Collection-Names)`);
  }
});

// set mappings; prioritizing CL args, then config, then default value
const mappings = {
  "default": {
    "index": db,
    "type": "$self"
  }
};
if (program.mappings) {
  Object.assign(mappings, require(program.mappings));
}
else if (CONFIG.mappings) {
  Object.assign(mappings, CONFIG.mappings);
}

const initOpts = {
  adminPort,
  bulkSize,
  db,
  url,
  mongoOpts,
  mappings,
  elasticOpts: CONFIG.elasticsearch,
  collections: CONFIG.mongo.collections,
  resumeTokenInterval: CONFIG.resumeTokenInterval,
  resumeTokenCollection: CONFIG.resumeTokenCollection,
  ignoreResumeTokensOnStart: CONFIG.ignoreResumeTokensOnStart,
  dumpOnStart: CONFIG.dumpOnStart
};

module.exports = initOpts;
