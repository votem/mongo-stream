const logger = new (require('service-logger'))(__filename);
const ResumeToken = require('./resumeToken');
const DumpProgress = require('./dumpProgress');
const versioning = require('./versioning');

class CollectionManager {
  constructor(collection) {
    this.collection = CollectionManager.db.collection(collection);
    this.collectionName = collection;
    this.resumeToken = new ResumeToken(collection);
    this.dumpProgress = new DumpProgress(collection);

    CollectionManager.elasticManager.setMappings(collection);
  }

  static initializeStaticVariables({db, elasticManager, dumpProgress, resumeToken}) {
    if (dumpProgress) {
      DumpProgress.storageCollection = db.collection(dumpProgress);
    }
    if (resumeToken) {
      ResumeToken.storageCollection = db.collection(resumeToken);
    }
    CollectionManager.dumpPause = {};
    CollectionManager.db = db;
    CollectionManager.elasticManager = elasticManager;
  }

  async dumpCollection() {
    // const todoQuery = this.dumpProgress.count
      // ? { _id: { $gt: this.dumpProgress.token } }
      // : {};
    //   let cursor = this.collection.find(todoQuery).batchSize(CollectionManager.elasticManager.bulkSize);

    let cursor = this.collection.find({"_id":{$gt:this.dumpProgress.token}}).batchSize(CollectionManager.elasticManager.bulkSize);
    const totalCount = (await cursor.count()) + this.dumpProgress.count;

    if (!await cursor.hasNext() && this.dumpProgress.completeDate) {
      logger.info(`${this.collectionName} already finished on ${this.dumpProgress.completeDate}`);
      return;
    }

    if (!await cursor.hasNext() && !this.dumpProgress.completeDate) {
      await this.dumpProgress.complete();
      logger.info(`${this.collectionName} has no more to do, marking as finished on ${this.dumpProgress.completeDate}`);
      return;
    }

    if (this.dumpProgress.completeDate) {
      logger.info(`${this.collectionName} was marked as finished but we found some more todo.`);
      await this.dumpProgress.notComplete();
    }

    let bulkOp = [];
    let nextObject;
    let startTime = new Date();
    let currentBulkRequest = Promise.resolve();

    while (await cursor.hasNext()) {
      if (CollectionManager.dumpPause.promise) {
        logger.info('Dump pause signal received');
        await CollectionManager.dumpPause.promise;
        logger.info('Dump resume signal received. Re-instantiating find cursor');
        cursor = this.collection.find({"_id":{$gt:this.dumpProgress.token}});
      }

      if (bulkOp.length !== 0 && bulkOp.length % (CollectionManager.elasticManager.bulkSize * 2) === 0) {
        let timeDelta = (new Date() - startTime) / 1000;
        let rate = ( (this.dumpProgress.count - this.dumpProgress.startCount) / timeDelta).toFixed(1);
        let eta = ( (totalCount - this.dumpProgress.count) / rate ).toFixed(1);
        let perc = (this.dumpProgress.count/totalCount).toFixed(1);

        logger.info(`${this.collectionName} progress: ${this.dumpProgress.count}/${totalCount} (${perc}%) -- ${rate} docs/sec -- eta ${eta}s`);
        this.dumpProgress.count += (bulkOp.length/2);
        await currentBulkRequest;
        currentBulkRequest = CollectionManager.elasticManager.sendBulkRequest(bulkOp);
        bulkOp = [];
        await this.dumpProgress.write();
      }

      nextObject = await cursor.next().catch(err => logger.error('next object error', err));
      if (nextObject === null) {
        break;
      }

      this.dumpProgress.token = nextObject._id;
      delete nextObject._id;
      bulkOp.push({
        index:  {
          _index: CollectionManager.elasticManager.mappings[this.collectionName].index,
          _type: CollectionManager.elasticManager.mappings[this.collectionName].type,
          _id: this.dumpProgress.token,
          _parent: nextObject[CollectionManager.elasticManager.mappings[this.collectionName].parentId],
          _versionType: CollectionManager.elasticManager.mappings[this.collectionName].versionType,
          _version: versioning.getVersionAsInteger(nextObject[CollectionManager.elasticManager.mappings[this.collectionName].versionField])
        }
      });
      bulkOp.push(nextObject);
    }
    this.dumpProgress.count += (bulkOp.length/2);
    logger.info(`${this.collectionName} FINAL request progress: ${this.dumpProgress.count}/${count}`);
    await currentBulkRequest;
    await CollectionManager.elasticManager.sendBulkRequest(bulkOp); // last bits
    await this.dumpProgress.complete();

    let finalDelta = ((new Date() - startTime) / 1000).toFixed(0);
    logger.info(`DONE dumping ${this.collectionName}, Execution Time: ${finalDelta}s`);
  }

  async watch(ignoreResumeToken = false) {
    logger.info(`new watcher for collection ${this.collectionName}`);
    if (ignoreResumeToken) {
      await this.resumeToken.reset();
    }

    const token = await this.resumeToken.get();

    this.changeStream = this.collection.watch({resumeAfter: token, fullDocument: 'updateLookup'});
    this._addChangeListener();
    this._addCloseListener();
    this._addErrorListener();
  }

  _addChangeListener() {
    this.changeStream.on('change', (change) => {
      if (change.operationType === 'invalidate') {
        logger.info(`${this.collectionName} invalidate`);
        this.resetChangeStream({dump: true, ignoreResumeToken: true});
        return;
      }

      this.resumeToken.token = change._id;
      CollectionManager.elasticManager.replicate(change);
    });
  }

  _addCloseListener() {
    this.changeStream.on('close', () => {
      logger.info(`the changestream for ${this.collectionName} has closed`);
    });
  }

  _addErrorListener() {
    this.changeStream.on('error', (error) => {
      logger.error(`${this.collectionName} changeStream error:`, error);
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
      this.resumeToken.reset();
      await CollectionManager.elasticManager.deleteElasticCollection(this.collectionName);
      await this.dumpCollection().catch(err => logger.error(`Error dumping collection:`, err));
    }
    if (ignoreResumeToken) {
      this.resumeToken.reset();
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
    this.resumeToken.write();
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
