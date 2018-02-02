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
  const responseBody = {total: changeStreams.length};
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
app.get('/add/:collections/:filter?', (request, response) => {
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
app.get('/remove/:collections/:filter?', (request, response) => {
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
app.get('/dump/:collections/:filter?', (request, response) => {
  mongoStream.filterCollections({
    filterArray: request.params.collections.split(','),
    filterType: request.params.filter
  }).then((collections) => {
    return mongoStream.dumpCollections(collections)
  })
    .then((results) => {
      response.send(results);
    });
});

// manually set the bulk size for replication testing
app.get('/bulk=:bulkSize', (request, response) => {
  response.send(`bulk size set from ${mongoStream.elasticManager.bulkSize} to ${request.params.bulkSize}`);
  mongoStream.elasticManager.bulkSize = Number(request.params.bulkSize);
});

app.listen(port, (err) => {
  if (err) {
    return console.log(`Error listening on ${port}: `, err)
  }

  // env config stuff
  let url = process.env.MONGO_RS;
  let mongoOpts = {poolSize: 100};

  // if MongoDB requires SSL connection, configure options here
  if (process.env.ROOT_FILE_PATH) {
    const ca = fs.readFileSync(process.env.ROOT_FILE_PATH);
    const cert = fs.readFileSync(process.env.KEY_FILE_PATH);
    const key = fs.readFileSync(process.env.KEY_FILE_PATH);

    Object.assign(mongoOpts, {
      ssl: true,
      sslCA: ca,
      sslKey: key,
      sslCert: cert
    });

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
    bulkSize: Number(process.env.BULK_SIZE),
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
