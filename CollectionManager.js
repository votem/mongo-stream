const BSON = require('bson');
const bson = new BSON();
const fs = require('fs');
const logger = new (require('service-logger'))(__filename);

class CollectionManager {
  constructor(db, collection, elasticManager, resumeTokenCollection) {
    this.db = db;
    this.elasticManager = elasticManager;
    this.collection = collection;
    this.elasticManager.setMappings(collection);
    this.resumeToken = null;
    this.resumeTokenCollection = resumeTokenCollection;
    this.changeStream;
  }
  async dumpCollection() {
    const cursor = this.db.collection(this.collection).find({}, {});
    const count = await this.db.collection(this.collection).count();
    let requestCount = 0;
    let bulkOp = [];
    let nextObject;
    let startTime = new Date();
    let currentBulkRequest = Promise.resolve();
    for (let i = 0; i < count; i++) {
      if (bulkOp.length !== 0 && bulkOp.length % (this.elasticManager.bulkSize * 2) === 0) {
        requestCount += (bulkOp.length/2);
        let currentTime = (new Date() - startTime) / 1000;
        await currentBulkRequest;
        currentBulkRequest = this.elasticManager.sendBulkRequest(bulkOp);
        bulkOp = [];
        logger.info(`${this.collection} request progress: ${requestCount}/${count} - ${(requestCount/currentTime).toFixed(2)} docs/sec`);
      }

      nextObject = await cursor.next().catch(err => logger.error(`next object error ${err}`));
      if (nextObject === null) {
        break;
      }

      let parentId = this.elasticManager.getParentId(nextObject, this.elasticManager.mappings[this.collection].type);

      const _id = nextObject._id;
      delete nextObject._id;
      bulkOp.push({
        index:  {
          _index: this.elasticManager.mappings[this.collection].index,
          _type: this.elasticManager.mappings[this.collection].type,
          _id: _id,
          _parent:parentId
        }
      });
      bulkOp.push(nextObject);
    }
    requestCount += (bulkOp.length/2);
    logger.info(`${this.collection} FINAL request progress: ${requestCount}/${count}`);
    await currentBulkRequest;
    await this.elasticManager.sendBulkRequest(bulkOp); // last bits
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
        this.resetChangeStream(true);
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
      this.resetChangeStream(false);
    });
  }

  async resetChangeStream(dump = false) {
    this.removeChangeStream();
    if (dump) {
      this.resumeToken = null;
      await this.elasticManager.deleteElasticCollection(this.collection);
      await this.dumpCollection().catch(err => logger.error(`Error dumping collection: ${err}`));
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

  hasResumeToken() {
    return !!this.resumeToken;
  }

  async getResumeToken() {
    if (!this.resumeToken) {
      if (this.resumeTokenCollection) {
        await this.getResumeTokenFromCollection();
      } else {
        this.getResumeTokenFromFile();
      }
    }

    return this.resumeToken;
  }

  async getResumeTokenFromCollection() {
    try {
      const { token } = await this.db.collection(this.resumeTokenCollection).findOne({ _id: this.collection })
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

  writeResumeToken() {
    if (!this.resumeToken) return;

    if (this.resumeTokenCollection) {
      this.writeResumeTokenToCollection()
    } else {
      this.writeResumeTokenToFile();
    }
  }

  writeResumeTokenToFile() {
    const b64String = bson.serialize(this.resumeToken).toString('base64');
    fs.writeFileSync(`./resumeTokens/${this.collection}`, b64String, 'base64');
    logger.debug(`resumeToken for collection ${this.collection} saved to disk`);
  }

  writeResumeTokenToCollection() {
    try {
      this.db.collection(this.resumeTokenCollection).updateOne(
        { _id: this.collection },
        { $set: { token: this.resumeToken }},
        { upsert: true },
      );
      logger.debug(`resumeToken for collection ${this.collection} saved to database`);
    } catch (err) {
      logger.debug(`resumeToken for collection ${this.collection} could not be saved to database`);
    }
  }

  removeResumeToken() {
    this.resumeToken = null;
    if(fs.existsSync(`./resumeTokens/${this.collection}`)) fs.unlink(`./resumeTokens/${this.collection}`);
  }

}

module.exports = CollectionManager;
