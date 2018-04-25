#!/usr/bin/env node

const express = require('express');
const app = express();
const bodyParser = require('body-parser');
app.use(bodyParser.json()); // for parsing application/json
const logger = new (require('service-logger'))(__filename);
const MongoStream = require('./mongo-stream');
let mongoStream;

const config = require('./configParser');

// returns the status of all collectionManagers currently running
app.get('/', (request, response) => {
  const collectionManagers = Object.values(mongoStream.collectionManagers);
  const responseBody = { total: collectionManagers.length };
  collectionManagers.forEach(manager => {
    if (manager.changeStream) {
      responseBody[manager.collection] = 'Listening';
    } else {
      responseBody[manager.collection] = 'Not Listening';
    }
  });

  response.send(responseBody);
});

// returns the status of all collectionManagers currently running
app.get('/mappings', (request, response) => {
  response.send(mongoStream.elasticManager.mappings);
});

app.post('/collection-manager?', (request, response) => {
  logger.info(request.body);
  const collections = request.body.collections;
  const managerOptions = {
    dump: request.body.dump,
    ignoreResumeTokens: request.body.ignoreResumeTokens,
    ignoreDumpProgress: request.body.ignoreDumpProgress,
    watch: request.body.watch
  };

  return mongoStream.addCollectionManager(collections, managerOptions)
    .then((results) => {
      response.send(results);
    }).catch(err => {
      logger.error(`Error posting collection-manager: ${err}`);
      response.send(err);
    });
});

// manually set the bulk size for replication testing
app.put('/bulk=:bulkSize', (request, response) => {
  response.send(`bulk size set from ${mongoStream.elasticManager.bulkSize} to ${request.params.bulkSize}`);
  mongoStream.elasticManager.bulkSize = Number(request.params.bulkSize);
});

// triggers a remove for the specified collections
app.delete('/collection-manager/:collections?', (request, response) => {
  const collections = request.params.collections.split(',');
  logger.info(`Deleting collections: ${collections}`);

  return mongoStream.removeCollectionManager(collections)
    .then(results => {
      logger.info(`Remaining collections after Delete: ${results}`);
      response.send(results);
    }).catch(err => {
      response.send(err);
    });
});

app.listen(config.adminPort, (err) => {
  if (err) {
    return logger.error(`Error listening on ${config.adminPort}: ${err}`);
  }

  MongoStream.init(config)
    .then((stream) => {
      logger.info('connected');
      mongoStream = stream;
    })
    .catch((err) => {
      logger.error(`Error Creating MongoStream: ${err.message}`);
      process.exit();
    });
  logger.info(`server is listening on port ${config.adminPort}`);
});
