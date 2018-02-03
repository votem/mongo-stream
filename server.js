#!/usr/bin/env node

const version = require('./package.json').version;
const program = require('commander');
program
  .version(version)
  .option('-c, --config [path]', 'Path to configuration file', './config/default.json')
  .option('--ssl', 'Enables connection to a mongod or mongos that has TLS/SSL support enabled')
  .option('--sslCAFile [path]', 'Specifies the .pem file that contains the root certificate chain from the Certificate Authority')
  .option('--sslPEMKeyFile [path]', 'Specifies the .pem file that contains both the TLS/SSL certificate and key')
  .parse(process.argv);

const CONFIG = require(program.config);
const SSL = program.ssl;
const SSL_CA_FILE = program.sslCAFile;
const SSL_PEM_KEY_FILE = program.sslPEMKeyFile;

const express = require('express');
const f = require('util').format;
const fs = require('fs');

const app = express();
const port = CONFIG.adminPort;

const MongoStream = require('./mongo-stream');
let mongoStream;

// returns the status of all change streams currently running
app.get('/', (request, response) => {
  const changeStreams = Object.keys(mongoStream.changeStreams);
  const responseBody = { total: changeStreams.length };
  for (let i = 0; i < changeStreams.length; i++) {
    const changeStream = mongoStream.changeStreams[changeStreams[i]];
    if (changeStream)
      responseBody[changeStream.collection] = 'Listening';
    else
      responseBody[changeStreams[i]] = 'Not Listening';
  }

  response.send(responseBody);
});

// triggers an add for the specified collections
// @param collections: comma-separated string of collections to operate on
// @param filter(optional): if exclusive, exclude the defined collections, else include them
app.put('/:collections/:filter?', (request, response) => {
  mongoStream.filterCollections({
    filterArray: request.params.collections.split(','),
    filterType: request.params.filter
  }).then((collections) => {
      return mongoStream.addChangeStreams(collections)
    })
    .then((results) => {
      response.send(results);
    });
});

// triggers a remove for the specified collections
// @param collections: comma-separated string of collections to operate on
// @param filter(optional): if exclusive, exclude the defined collections, else include them
app.delete('/:collections/:filter?', (request, response) => {
  mongoStream.filterCollections({
    filterArray: request.params.collections.split(','),
    filterType: request.params.filter
  }).then((collections) => {
    return mongoStream.removeChangeStreams(collections)
  })
    .then((results) => {
      response.send(results);
    });
});

// triggers a dump for the specified collections
// @param collections: comma-separated string of collections to operate on
// @param filter(optional): if exclusive, exclude the defined collections, else include them
app.post('/dump/:collections/:filter?', (request, response) => {
  mongoStream.filterCollections({
    filterArray: request.params.collections.split(','),
    filterType: request.params.filter
  }).then((collections) => {
    const ignoreResumeTokens = request.query.force || false;
    return mongoStream.dumpCollections(collections, ignoreResumeTokens);
  })
    .then((results) => {
      response.send(results);
    });
});

// manually set the bulk size for replication testing
app.put('/bulk=:bulkSize', (request, response) => {
  response.send(`bulk size set from ${mongoStream.elasticManager.bulkSize} to ${request.params.bulkSize}`);
  mongoStream.elasticManager.bulkSize = Number(request.params.bulkSize);
});

app.listen(port, (err) => {
  if (err) {
    return console.log(`Error listening on ${port}: `, err)
  }

  // config
  const db = CONFIG.mongo.database;
  const elasticOpts = CONFIG.elasticsearch;
  const url = f('mongodb://%s@%s', CONFIG.mongo.user, CONFIG.mongo.url)
  let mongoOpts = CONFIG.mongo.options;


  // if MongoDB requires SSL connection, configure options here
  if (SSL) {
    const ca = fs.readFileSync(SSL_CA_FILE);
    const cert = fs.readFileSync(SSL_PEM_KEY_FILE);
    const key = fs.readFileSync(SSL_PEM_KEY_FILE);

    Object.assign(mongoOpts, {
      ssl: true,
      sslCA: ca,
      sslKey: key,
      sslCert: cert
    });
  }

  const initOpts = {
    url,
    mongoOpts,
    db,
    elasticOpts,
    bulkSize: CONFIG.bulkSize,
    mappings: {
      "default": {
        "index": db,
        "type": "$self"
      }
    },
    collections: CONFIG.mongo.collections,
    resumeTokenInterval: CONFIG.resumeTokenInterval
  };

  MongoStream.init(initOpts)
    .then((stream) => {
      console.log('connected');
      mongoStream = stream;
      const dumpOnStart = CONFIG.dumpOnStart;
      const collections = CONFIG.mongo.collections;
      if (dumpOnStart){
        const ignoreResumeTokens = CONFIG.ignoreResumeTokensOnStart;
        mongoStream.dumpCollections(collections, ignoreResumeTokens);
      }

      mongoStream.addChangeStreams(collections);
    })
    .catch((err) => {
      console.log(`Error Creating MongoStream: ${err.message}`);
      process.exit();
    });
  console.log(`server is listening on port ${port}`);
});
