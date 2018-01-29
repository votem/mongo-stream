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

// triggers a manual collection dump for the specified collection
app.get('/dump/:collection', (request, response) => {
  console.log(`dumping collection ${request.params.collection} to ES`);
  mongoStream.collectionDump(request.params.collection);
  response.send(`dumping collection ${request.params.collection} to ES`);
});

// starts listening to the change stream of the specified collection
app.get('/start/:collection', (request, response) => {
  mongoStream.addChangeStream(request.params.collection).then(() => {
    response.send(`${request.params.collection} change stream added`);
  });
});

// stops listening to the change stream of the specified collection
app.get('/stop/:collection', (request, response) => {
  mongoStream.removeChangeStream(request.params.collection);
  response.send(`${request.params.collection} change stream removed`);
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
    dumpLimit: Number(process.env.BULK_SIZE)
  };

  // parse inclusive/exclusive collection list, set up filtering before adding a change stream
  if (process.env.COLL_INCLUSIVE) {
    initOpts.coll_inclusive = JSON.parse(process.env.COLL_INCLUSIVE);
  }
  else if (process.env.COLL_EXCLUSIVE) {
    initOpts.coll_exclusive = JSON.parse(process.env.COLL_EXCLUSIVE);
  }

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
