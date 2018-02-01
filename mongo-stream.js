const MongoClient = require('mongodb').MongoClient;

const ElasticManager = require('./elasticManager');
const ChangeStream = require('./changeStream');


class MongoStream {
  constructor(elasticManager, db) {
    this.elasticManager = elasticManager;
    this.db = db;
    this.changeStreams = {};

    // write resume tokens to file every minute
    setInterval(() => {
      const changeStreams = Object.keys(this.changeStreams);
      for (let i = 0; i < changeStreams.length; i++) {
        const changeStream = this.changeStreams[changeStreams[i]];
        if (changeStream) changeStream.writeResumeToken();
      }
    }, 60000);
  }

  // constructs and returns a new MongoStream
  static async init(options) {
    const client = await MongoClient.connect(options.url, options.mongoOpts);
    const db = client.db(options.db);
    const elasticManager = new ElasticManager(options.elasticOpts, options.mappings, options.bulkSize);
    return new MongoStream(elasticManager, db);
  }

  async filterCollections(opts) {
    let filteredCollections;
    if (!opts.filterType || opts.filterType === 'inclusive') {
      filteredCollections = opts.filterArray;
    }
    else if (opts.filterType === 'exclusive') {
      const mongoCollections = await this.db.collections();
      const collections = [];
      for (let i = 0; i < mongoCollections.length; i++) {
        if (opts.filterArray.indexOf(mongoCollections[i].collectionName) === -1)
          collections.push(mongoCollections[i].collectionName);
      }
      filteredCollections = collections;
    }
    else return `Unsupported Filter: ${opts.filterType}`;
    return filteredCollections;
  }

  async addChangeStreams(collections) {
    await this.removeChangeStreams(collections);

    for (let i = 0; i < collections.length; i++) {
      this.changeStreams[collections[i]] = new ChangeStream(this.db, collections[i]);
      this.changeStreams[collections[i]].listen(this.elasticManager);
    }

  }

  async removeChangeStreams(collections) {
    for (let i = 0; i < collections.length; i++) {
      if (this.changeStreams[collections[i]]) {
        await this.changeStreams[collections[i]].remove();
        delete this.changeStreams[collections[i]];
        this.changeStreams[collections[i]] = null;
      }
    }
  }

  async dumpCollections(collections) {
    for (let i = 0; i < collections.length; i++) {
      this.changeStreams[collections[i]].removeResumeToken();
      await this.elasticManager.deleteElasticCollection(collections[i]);

      let limit = 100000;
      const cursor = this.db.collection(collections[i]).find({}, { skip: 0, limit: limit });
      const count = await this.db.collection(collections[i]).count();
      if (count < limit) { limit = count }
      await this.elasticManager.replicateElasticCollection(cursor, collections[i], count, limit);

    }
  }

}

module.exports = MongoStream;
