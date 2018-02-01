const express = require('express');
const f = require('util').format;
const fs = require('fs');

const app = express();
const port = process.env.MS_ADMIN_PORT || '3000';

const MongoStream = require('./mongo-stream');
let mongoStream;

// returns the status of all change streams currently running
app.get('/', (request, response) => {
  const changeStreams = Object.keys(mongoStream.changeStreams);
  const responseBody = {};
  for (let i = 0; i < changeStreams.length; i++) {
    if (mongoStream.changeStreams[changeStreams[i]]) {
      responseBody[changeStreams[i]] = 'Listening'
    }
    else {
      responseBody[changeStreams[i]] = 'Not Listening'
    }
  }

  response.send(responseBody);
});

// triggers an operation for the specified collections
// @param op: operation to perform (dump, add, or remove)
// @param collections: comma-separated string of collections to operate on
// @param filter(optional): if exclusive, exclude the defined collections, else include them
app.get('/:op/:collections/:filter?', (request, response) => {
  mongoStream.filterAndExecute({
    filterArray: request.params.collections.split(','),
    filterType: request.params.filter,
    operation: request.params.op
  }).then((results) => {
    response.send(results);
  });
});

// manually set the bulk size for replication testing
app.get('/bulk=:bulksize', (request, response) => {
  response.send(`bulk size set from ${mongoStream.dumpLimit} to ${request.params.bulksize}`);
  mongoStream.dumpLimit = Number(request.params.bulksize);
});

app.listen(port, (err) => {
  if (err) {
    return console.log(`Error listening on ${port}: `, err)
  }

  // env config stuff
  let url = process.env.MONGO_RS;
  let mongoOpts = {};

  // if MongoDB requires SSL connection, configure options here
  if (process.env.ROOT_FILE_PATH) {
    const ca = fs.readFileSync(process.env.ROOT_FILE_PATH);
    const cert = fs.readFileSync(process.env.KEY_FILE_PATH);
    const key = fs.readFileSync(process.env.KEY_FILE_PATH);

    mongoOpts = {
      ssl: true,
      sslCA: ca,
      sslKey: key,
      sslCert: cert
    };

    const user = encodeURIComponent(process.env.MONGO_USER);
    url = f('mongodb://%s@%s', user, process.env.MONGO_RS)
  }

  const initOpts = {
    url,
    mongoOpts,
    db: process.env.MONGO_DB,
    elasticOpts: {
      host: process.env.ELASTIC_HOST,
      apiVersion: process.env.ELASTIC_API
    },
    dumpLimit: Number(process.env.BULK_SIZE),
    mappings: require(process.env.MAPPINGS)
  };

  MongoStream.init(initOpts)
    .then((stream) => {
      console.log('connected');
      mongoStream = stream;
    })
    .catch((err) => {
      console.log(`Error Creating MongoStream: ${err.message}`);
      process.exit();
    });
  console.log(`server is listening on port ${port}`);
});
