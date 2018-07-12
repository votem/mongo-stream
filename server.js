#!/usr/bin/env node

const express = require('express');
const app = express();
const bodyParser = require('body-parser');
app.use(bodyParser.json()); // for parsing application/json
const logger = new (require('service-logger'))(__filename);
const MongoStream = require('./lib/mongo-stream');
const CollectionManager = require('./lib/CollectionManager');
let mongoStream;

const config = require('./lib/configParser');

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

// returns the mappings of all collectionManagers currently running
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
      logger.error(`Error posting collection-manager:`, err);
      response.send(err);
    });
});

// toggle dump process
app.put('/dump/:toggle', (request, response) => {
  switch(request.params.toggle) {
    case 'pause':
      CollectionManager.pauseDump();
      response.send('Dump paused. To resume, use "/dump/resume"');
      break;
    case 'resume':
      CollectionManager.resumeDump();
      response.send('Dump resumed.');
      break;
    default:
      response.send(`ERROR: unknown dump option "${request.params.toggle}"`);
      break;
  }
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
    return logger.error(`Error listening on ${config.adminPort}:`, err);
  }

  MongoStream.init(config)
    .then((stream) => {
      logger.info('connected');
      mongoStream = stream;
    })
    .catch((err) => {
      logger.error(`Error Creating MongoStream:`, err);
      process.exit();
    });
  logger.info(`server is listening on port ${config.adminPort}`);
});
