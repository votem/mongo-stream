const MongoClient = require('mongodb').MongoClient;
const ElasticManager = require('./elasticManager');
const CollectionManager = require('./CollectionManager');
const logger = new (require('service-logger'))(__filename);

class MongoStream {
  constructor(elasticManager, db, resumeTokenInterval = 60000) {
    this.elasticManager = elasticManager;
    this.db = db;
    this.collectionManagers = {};

    // after successful reconnection to mongo, restart all change streams
    db.on('reconnect', () => {
      logger.info('connection reestablished with mongoDB');
      const collectionManagers = Object.values(this.collectionManagers);
      collectionManagers.forEach(manager => {
        manager.getResumeToken();
        manager.resetChangeStream({dump: false, ignoreResumeToken: false});
      });
    });

    // write resume tokens to file on an interval
    setInterval(() => {
      this.writeAllResumeTokens();
    }, resumeTokenInterval);
  }

  // constructs and returns a new MongoStream
  static async init(options) {
    const client = await MongoClient.connect(options.url, options.mongoOpts);
    const db = client.db(options.db);
    // log any db events emitted
    db.on('close', (log) => {logger.info(`close ${log}`)});
    db.on('error', (err) => {logger.error(`db Error: ${err}`)});
    db.on('parseError', (err) => {logger.error(`db parseError ${err}`)});
    db.on('timeout', (err) => {
      logger.error(`db timeout ${err}`);
      this.writeAllResumeTokens();
      process.exit();
    });

    await db.createCollection('init');  // workaround for "MongoError: cannot open $changeStream for non-existent database"
    await db.dropCollection('init');
    const elasticManager = new ElasticManager(options.elasticOpts, options.mappings, options.bulkSize, options.parentChildRelations);
    const resumeTokenInterval = options.resumeTokenInterval;
    const mongoStream = new MongoStream(elasticManager, db, resumeTokenInterval);
    const managerOptions = {
      dump: options.dumpOnStart,
      ignoreResumeTokens: options.ignoreResumeTokensOnStart,
      watch: true
    };

    await mongoStream.addCollectionManager(options.collections, managerOptions);

    return mongoStream;
  }

  writeAllResumeTokens() {
    const collectionManagers = Object.values(this.collectionManagers);
    collectionManagers.forEach(manager => {
      manager.writeResumeToken();
    });
  }

  // accepts single collection or array
  async addCollectionManager(collections, options) {
    if (!Array.isArray(collections)) collections = [collections];
    await this.removeCollectionManager(collections);

    for (const collection of collections) {
      const collectionManager = new CollectionManager(this.db, collection, this.elasticManager);
      if (options.dump) {
        await collectionManager.getDumpProgress();

        if (options.ignoreDumpProgress || collectionManager.dumpProgress.count === 0) {
          await collectionManager.elasticManager.deleteElasticCollection(collection);
          collectionManager.resetDumpProgress();
        }
        await collectionManager.dumpCollection();
      }
      if (options.watch) { collectionManager.watch(options.ignoreResumeTokens) }

      this.collectionManagers[collection] = collectionManager;
    }
  }

  async removeCollectionManager(collections) {
    if (!Array.isArray(collections)) collections = [collections];
    for (const collection of collections) {
      if (this.collectionManagers[collection]) {
        this.collectionManagers[collection].removeChangeStream();
        delete this.collectionManagers[collection];
      }
    }

    return Object.keys(this.collectionManagers);
  }
}

module.exports = MongoStream;
