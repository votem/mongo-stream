const MongoClient = require('mongodb').MongoClient;

const ElasticManager = require('./elasticManager');
const CollectionManager = require('./CollectionManager');


class MongoStream {
  constructor(elasticManager, db, resumeTokenInterval = 60000) {
    this.elasticManager = elasticManager;
    this.db = db;
    this.collectionManagers = {};

    // write resume tokens to file every minute
    setInterval(() => {
      const collectionManagers = Object.values(this.collectionManagers);
      collectionManagers.forEach(manager => {
        manager.writeResumeToken();
      });
    }, resumeTokenInterval);
  }

  // constructs and returns a new MongoStream
  static async init(options) {
    const client = await MongoClient.connect(options.url, options.mongoOpts);
    const db = client.db(options.db);
    await db.createCollection('init');  // workaround for "MongoError: cannot open $changeStream for non-existent database"
    await db.dropCollection('init');
    const elasticManager = new ElasticManager(options.elasticOpts, options.mappings, options.bulkSize);
    const resumeTokenInterval = options.resumeTokenInterval;
    const mongoStream = new MongoStream(elasticManager, db, resumeTokenInterval);
    const managerOptions = {
      dump: options.dumpOnStart,
      ignoreResumeTokens: options.ignoreResumeTokensOnStart,
      watch: true
    }

    await mongoStream.addCollectionManager(options.collections, managerOptions);

    return mongoStream;
  }

  // accepts single collection or array
  async addCollectionManager(collections, options) {
    if (!Array.isArray(collections)) collections = [collections];
    await this.removeCollectionManager(collections);

    for (const collection of collections) {
      const collectionManager = new CollectionManager(this.db, collection, this.elasticManager);
      if (options.dump) {
        await collectionManager.elasticManager.deleteElasticCollection(collection);
        await collectionManager.dumpCollection(options.ignoreResumeTokens);
      }
      if (options.watch) { collectionManager.watch() };

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
