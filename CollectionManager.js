const BSON = require('bson');
const bson = new BSON();
const fs = require('fs');

class CollectionManager {
  constructor(db, collection, elasticManager) {
    this.db = db;
    this.elasticManager = elasticManager;
    this.collection = collection;
    this.elasticManager.setMappings(collection);
    this.resumeToken = this.getResumeToken();
    this.changeStream;
  }

  async dumpCollection() {
    const cursor = this.db.collection(this.collection).find({}, {});
    const count = await this.db.collection(this.collection).count();
    let requestCount = 0;
    let bulkOp = [];
    let nextObject;
    for (let i = 0; i < count; i++) {
      if (bulkOp.length !== 0 && bulkOp.length % (this.elasticManager.bulkSize * 2) === 0) {
        requestCount += (bulkOp.length/2);
        console.log(`${this.collection} request progress: ${requestCount}/${count}`);
        await this.elasticManager.sendBulkRequest(bulkOp);
        bulkOp = [];
      }

      nextObject = await cursor.next().catch(err => console.log('next object error', err));
      if (nextObject === null) {
        break;
      }

      const _id = nextObject._id;
      delete nextObject._id;
      bulkOp.push({
        index:  {
          _index: this.elasticManager.mappings[this.collection].index,
          _type: this.elasticManager.mappings[this.collection].type,
          _id: _id
        }
      });
      bulkOp.push(nextObject);
    }
    requestCount += (bulkOp.length/2);
    console.log(`${this.collection} FINAL request progress: ${requestCount}/${count}`);
    await this.elasticManager.sendBulkRequest(bulkOp); // last bits
    console.log('done');
  }

  watch(ignoreResumeToken=false) {
    console.log('new watcher for collection', this.collection);
    if (ignoreResumeToken) this.resumeToken = null;
    this.changeStream = this.db.collection(this.collection).watch({resumeAfter: this.resumeToken, fullDocument: 'updateLookup'});
    this._addChangeListener();
    this._addCloseListener();
    this._addErrorListener();
  }

  _addChangeListener() {
    this.changeStream.on('change', (change) => {
      console.log('change event', change.operationType);
      if (change.operationType === 'invalidate') {
        this.resetChangeStream();
        return;
      }

      this.resumeToken = change._id;
      this.elasticManager.replicate(change);
    });
  }

  _addCloseListener() {
    this.changeStream.on('close', async () => {
      console.log('close event');
    });
  }

  _addErrorListener() {
    this.changeStream.on('error', (error) => {
      console.log('changeStream error', error);
    });
  }

  async resetChangeStream() {
    delete this.changeStream;
    this.resumeToken = null;
    await this.elasticManager.deleteElasticCollection(this.collection);
    await this.dumpCollection().catch(err => console.log(err));
    this.watch();
  }

  removeChangeStream() {
    if (!this.changeStream) { return; }

    const listeners = this.changeStream.eventNames();
    listeners.forEach(listener => {
      this.changeStream.removeAllListeners(listener);
    });
    delete this.changeStream;
    this.resumeToken = null;
  }

  getResumeToken() {
    if (!this.resumeToken) {
      try {
        const base64Buffer = fs.readFileSync(`./resumeTokens/${this.collection}`);
        this.resumeToken = bson.deserialize(base64Buffer);
      } catch (err) {
        this.resumeToken = null;
      }
    }

    return this.resumeToken;
  }

  hasResumeToken() {
    return this.resumeToken ? true : false;
  }

  writeResumeToken() {
    if (!this.resumeToken) return;
    const b64String = bson.serialize(this.resumeToken).toString('base64');
    fs.writeFileSync(`./resumeTokens/${this.collection}`, b64String, 'base64');
  }

  removeResumeToken() {
    this.resumeToken = null;
    if(fs.existsSync(`./resumeTokens/${this.collection}`)) fs.unlink(`./resumeTokens/${this.collection}`);
  }

}

module.exports = CollectionManager;
