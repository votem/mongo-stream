#!/usr/bin/env node

const express = require('express');
const app = express();

const MongoStream = require('./mongo-stream');
let mongoStream;

const config = require('./configParser');

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
  const collections = request.params.collections.split(',');
  mongoStream.filterCollections(collections, request.params.filter)
    .then((collections) => {
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
  const collections = request.params.collections.split(',');
  mongoStream.filterCollections(collections, request.params.filter)
    .then((collections) => {
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
  const collections = request.params.collections.split(',');
  mongoStream.filterCollections(collections, request.params.filter)
    .then((collections) => {
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

app.listen(config.adminPort, (err) => {
  if (err) {
    return console.log(`Error listening on ${config.adminPort}: `, err)
  }

  MongoStream.init(config)
    .then((stream) => {
      console.log('connected');
      mongoStream = stream;
    })
    .catch((err) => {
      console.log(`Error Creating MongoStream: ${err.message}`);
      process.exit();
    });
  console.log(`server is listening on port ${config.adminPort}`);
});
