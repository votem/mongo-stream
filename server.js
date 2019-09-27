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

// TODO: completely untested
app.post('/sync-managers', (request, response) => {
  const collectionsSynced = mongoStream.syncCollectionManagers(request.body);
  response.send(collectionsSynced);
});

// returns the mappings of all collectionManagers currently running
app.get('/es-mappings', (request, response) => {
  response.send(mongoStream.elasticManager.mappings);
});

app.get('/es-mappings/:collection', (request, response) => {
  response.send(mongoStream.elasticManager.mappings[request.params.collection]);
});

app.get('/load-es-mappings', (request, response) => {
  mongoStream.elasticManager.loadESFields().then((r) => response.send(r));
});

// TODO: this probably is skipping some steps
app.post('/load-es-mappings', (request, response) => {
  logger.info(request.body);

  _.forEach(request.body.mappings, (colMap, col) => {
    // mongoStream.elasticManager.mappings[col].fields = [];
    const fields = [];

    _.deepMapValues(colMap.properties, (val,path) => {
      fields.push(path.replace(/properties\./g, '').replace(/\.\w+$/, '') );
    });

    mongoStream.elasticManager.mappings[col].fields  = _.filter(_.uniq(fields), (f) => !f.includes('_')).sort();
  });

  response.send(_.mapValues(mongoStream.elasticManager.mappings, 'fields'));
});


app.post('/collection-manager?', (request, response) => {
  logger.info(request.body);
  const collections = request.body.collections;
  const managerOptions = {
    dump: request.body.dump,
    ignoreResumeTokens: request.body.ignoreResumeTokens,
    ignoreDumpProgress: request.body.ignoreDumpProgress,
    watch: request.body.watch,
    loadESMappings: request.body.loadESMappings
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
