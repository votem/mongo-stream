const BSON = require('bson');
const bson = new BSON();
const fs = require('fs');
const logger = new (require('service-logger'))(__filename);
const ObjectId = require('mongodb').ObjectId;


class CollectionManager {
  constructor(db, collection, elasticManager) {
    this.db = db;
    this.elasticManager = elasticManager;
    this.collection = collection;
    this.elasticManager.setMappings(collection);
    this.resumeToken = null;
    this.dumpProgress = null;
  }

  static initializeStaticVariables(dumpProgress, resumeToken) {
    CollectionManager.dumpProgressCollection = dumpProgress;
    CollectionManager.resumeTokenCollection = resumeToken;
    CollectionManager.dumpPause = {};
  }

  async dumpCollection() {
    let cursor = this.db.collection(this.collection).find({"_id":{$gt:ObjectId(this.dumpProgress.token)}});
    const count = (await cursor.count()) + this.dumpProgress.count;

    let bulkOp = [];
    let nextObject;
    let startTime = new Date();
    let currentBulkRequest = Promise.resolve();

    // TODO: the params in this for loop are nonsense, we should turn this into a while loop, but I don't want to rock the boat in this commit
    for (let i = 0; i < count; i++) {
      if (CollectionManager.dumpPause.promise) {
        logger.info('Dump pause signal received');
        await CollectionManager.dumpPause.promise;
        logger.info('Dump resume signal received. Re-instantiating find cursor');
        cursor = this.db.collection(this.collection).find({"_id":{$gt:ObjectId(this.dumpProgress.token)}});
      }

      if (bulkOp.length !== 0 && bulkOp.length % (this.elasticManager.bulkSize * 2) === 0) {
        let currentTime = (new Date() - startTime) / 1000;
        logger.info(`${this.collection} request progress: ${this.dumpProgress.count}/${count} - ${(this.dumpProgress.count/currentTime).toFixed(2)} docs/sec`);
        this.dumpProgress.count += (bulkOp.length/2);
        await currentBulkRequest;
        currentBulkRequest = this.elasticManager.sendBulkRequest(bulkOp);
        bulkOp = [];
        await this.writeDumpProgress();
      }

      nextObject = await cursor.next().catch(err => logger.error(`next object error ${err}`));
      if (nextObject === null) {
        break;
      }

      this.dumpProgress.token = nextObject._id;
      delete nextObject._id;
      bulkOp.push({
        index:  {
          _index: this.elasticManager.mappings[this.collection].index,
          _type: this.elasticManager.mappings[this.collection].type,
          _id: this.dumpProgress.token,
          _parent: nextObject[this.elasticManager.mappings[this.collection].parentId]
        }
      });
      bulkOp.push(nextObject);
    }
    this.dumpProgress.count += (bulkOp.length/2);
    logger.info(`${this.collection} FINAL request progress: ${this.dumpProgress.count}/${count}`);
    await currentBulkRequest;
    await this.elasticManager.sendBulkRequest(bulkOp); // last bits
    this.resetDumpProgress();
    await this.writeDumpProgress();
    logger.info('done');
  }

  async watch(ignoreResumeToken = false) {
    logger.info(`new watcher for collection ${this.collection}`);
    if (ignoreResumeToken) {
      this.resumeToken = null;
    } else {
      await this.getResumeToken();
    }

    this.changeStream = this.db.collection(this.collection).watch({resumeAfter: this.resumeToken, fullDocument: 'updateLookup'});
    this._addChangeListener();
    this._addCloseListener();
    this._addErrorListener();
  }

  _addChangeListener() {
    this.changeStream.on('change', (change) => {
      if (change.operationType === 'invalidate') {
        logger.info(`${this.collection} invalidate`);
        this.resetChangeStream({dump: true, ignoreResumeToken: true});
        return;
      }

      this.resumeToken = change._id;
      this.elasticManager.replicate(change);
    });
  }

  _addCloseListener() {
    this.changeStream.on('close', () => {
      logger.info(`the changestream for ${this.collection} has closed`);
    });
  }

  _addErrorListener() {
    this.changeStream.on('error', (error) => {
      logger.error(`${this.collection} changeStream error: ${error}`);
      // 40585: resume of change stream was not possible, as the resume token was not found
      // 40615: The resume token UUID does not exist. Has the collection been dropped?
      if (error.code === 40585 || error.code === 40615) {
        this.resetChangeStream({ignoreResumeToken: true});
      }
      else {
        this.resetChangeStream({ignoreResumeToken: false});
      }
    });
  }

  async resetChangeStream({dump, ignoreResumeToken}) {
    this.removeChangeStream();
    if (dump) {
      this.resumeToken = null;
      await this.elasticManager.deleteElasticCollection(this.collection);
      await this.dumpCollection().catch(err => logger.error(`Error dumping collection: ${err}`));
    }
    if (ignoreResumeToken) {
      this.resumeToken = null;
    }
    this.watch();
  }

  removeChangeStream() {
    if (!this.changeStream) { return; }

    const listeners = this.changeStream.eventNames();
    listeners.forEach(listener => {
      this.changeStream.removeAllListeners(listener);
    });
    delete this.changeStream;
    this.writeResumeToken();
  }

  async getResumeToken() {
    if (!this.resumeToken) {
      if (CollectionManager.resumeTokenCollection) {
        await this.getResumeTokenFromCollection();
      } else {
        this.getResumeTokenFromFile();
      }
    }

    return this.resumeToken;
  }

  async getResumeTokenFromCollection() {
    try {
      const { token } = await this.db.collection(CollectionManager.resumeTokenCollection).findOne({ _id: this.collection });
      this.resumeToken = token;
    } catch (err) {
      logger.debug(`resumeToken for ${this.collection} could not be retrieved from database`);
      this.resumeToken = null;
    }
  }

  getResumeTokenFromFile() {
    try {
      const base64Buffer = fs.readFileSync(`./resumeTokens/${this.collection}`);
      this.resumeToken = bson.deserialize(base64Buffer);
    } catch (err) {
      this.resumeToken = null;
    }
  }

  async getDumpProgress() {
    if (!this.dumpProgress) {
      if (CollectionManager.dumpProgressCollection) {
        await this.getDumpProgressFromCollection();
      } else {
        this.getDumpProgressFromFile();
      }
    }

    return this.dumpProgress;
  }

  getDumpProgressFromFile() {
    try {
      console.log('dump prog - ');
      this.dumpProgress = JSON.parse(fs.readFileSync(`./dumpProgress/${this.collection}`, 'utf8'));
    } catch (err) {
      this.resetDumpProgress();
    }
  }

  async writeDumpProgress() {
    if (CollectionManager.dumpProgressCollection) {
      await this.writeDumpProgressToCollection();
    } else {
      this.writeDumpProgressToFile();
    }
  }


  writeDumpProgressToFile() {
    fs.writeFileSync(`./dumpProgress/${this.collection}`, JSON.stringify(this.dumpProgress));
    logger.debug(`dumpProgress for collection ${this.collection} saved to disk`);
  }

  async writeDumpProgressToCollection() {
    try {
      await this.db.collection(CollectionManager.dumpProgressCollection).updateOne(
        { _id: this.collection },
        { $set: { token: this.dumpProgress.token, count: this.dumpProgress.count }},
        { upsert: true },
      );
      logger.debug(`resumeToken for collection ${this.collection} saved to database`);
    } catch (err) {
      logger.debug(`resumeToken for collection ${this.collection} could not be saved to database`);
    }
  }

  resetDumpProgress() {
    this.dumpProgress = {
      token: ObjectId('000000000000000000000000'),
      count: 0
    }
  }

  async getDumpProgressFromCollection() {
    try {
      const dumpProgress = await this.db.collection(CollectionManager.dumpProgressCollection).findOne({ _id: this.collection });
      this.dumpProgress = {
        token: dumpProgress.token,
        count: dumpProgress.count
      };
    } catch (err) {
      logger.debug(`dumpProgress for ${this.collection} could not be retrieved from database`);
      this.resetDumpProgress();
    }
  }


  async writeResumeToken() {
    if (!this.resumeToken) return;

    if (CollectionManager.resumeTokenCollection) {
      await this.writeResumeTokenToCollection();
    } else {
      this.writeResumeTokenToFile();
    }
  }

  writeResumeTokenToFile() {
    const b64String = bson.serialize(this.resumeToken).toString('base64');
    fs.writeFileSync(`./resumeTokens/${this.collection}`, b64String, 'base64');
    logger.debug(`resumeToken for collection ${this.collection} saved to disk`);
  }

  async writeResumeTokenToCollection() {
    try {
      await this.db.collection(CollectionManager.resumeTokenCollection).updateOne(
        { _id: this.collection },
        { $set: { token: this.resumeToken }},
        { upsert: true },
      );
      logger.debug(`resumeToken for collection ${this.collection} saved to database`);
    } catch (err) {
      logger.debug(`resumeToken for collection ${this.collection} could not be saved to database`);
    }
  }

  static pauseDump() {
    if (!CollectionManager.dumpPause.promise) {
      CollectionManager.dumpPause.promise = new Promise((resolve) => {
        CollectionManager.dumpPause.resolve = resolve;
      });
    }
  }

  static resumeDump() {
    if (CollectionManager.dumpPause.promise) {
      CollectionManager.dumpPause.resolve();
      CollectionManager.dumpPause.promise = null;
      CollectionManager.dumpPause.resolve = null;
    }
  }

}

module.exports = CollectionManager;
