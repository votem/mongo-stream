const elasticsearch = require('elasticsearch');

class ElasticManager {
  constructor(elasticOpts, mappings, bulkSize) {
    this.esClient = new elasticsearch.Client(elasticOpts);
    this.mappings = mappings;
    this.bulkSize = bulkSize;
    this.bulkOp = [];
    this.interval = null;

  }

  // Calls the appropriate replication function based on the change object parsed from a change stream
  replicate(change) {
    if (!this.interval) {
      this.interval = setInterval(() => {
        clearInterval(this.interval);
        this.interval = null;
        console.log(this.bulkOp.length);
        this.sendBulkRequest(this.bulkOp);
        this.bulkOp = [];
      }, 500);
    }

    const replicationFunctions = {
      'insert': this.insertDoc,
      'update': this.insertDoc,
      'replace': this.insertDoc,
      'delete': this.deleteDoc
    };
    if (replicationFunctions.hasOwnProperty(change.operationType)) {
      this.setMappings(change.ns.coll);
      console.log(`- ${change.documentKey._id.toString()}: ${change.ns.coll} ${change.operationType}`);
      return replicationFunctions[change.operationType].call(this, change);
    }
    else {
      console.log(`REPLICATION ERROR: ${change.operationType} is not a supported function`);
    }
  }

// insert event format https://docs.mongodb.com/manual/reference/change-events/#insert-event
  insertDoc(changeStreamObj) {
    const esId = changeStreamObj.fullDocument._id.toString(); // convert mongo ObjectId to string
    delete changeStreamObj.fullDocument._id;
    const esReadyDoc = changeStreamObj.fullDocument;

    this.bulkOp.push({
        index:  {
          _index: this.mappings[changeStreamObj.ns.coll].index,
          _type: this.mappings[changeStreamObj.ns.coll].type,
          _id: esId
        }
      });
    this.bulkOp.push(esReadyDoc);
  }

  deleteDoc(changeStreamObj) {
    const esId = changeStreamObj.documentKey._id.toString(); // convert mongo ObjectId to string

    this.bulkOp.push({
      delete: {
        _index: this.mappings[changeStreamObj.ns.coll].index,
        _type: this.mappings[changeStreamObj.ns.coll].type,
        _id: esId
      }
    });
  }

  async replicateElasticCollection(cursor, collection, count) {
    let requestCount = 0;
    let bulkOp = [];
    let nextObject;
    for (let i = 0; i < count; i++) {
      if (bulkOp.length !== 0 && bulkOp.length % (this.bulkSize * 2) === 0) {
        requestCount += (bulkOp.length/2);
        console.log(`${collection} request progress: ${requestCount}/${count}`);
        await this.sendBulkRequest(bulkOp);
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
          _index: this.mappings[collection].index,
          _type: this.mappings[collection].type,
          _id: _id
        }
      });
      bulkOp.push(nextObject);
    }
    requestCount += (bulkOp.length/2);
    console.log(`${collection} FINAL request progress: ${requestCount}/${count}`);
    await this.sendBulkRequest(bulkOp); // last bits
    console.log('done');
  }

  // delete all docs in ES before dumping the new docs into it
  async deleteElasticCollection(collectionName) {
    this.setMappings(collectionName);

    let searchResponse;
    try {
      // First get a count for all ES docs of the specified type
      searchResponse = await this.esClient.search({
        index: this.mappings[collectionName].index,
        type: this.mappings[collectionName].type,
        size: this.bulkSize,
        scroll: '1m'
      });
    }
    catch (err) {
      // if the search query failed, the index or type does not exist
      searchResponse = {hits: {total: 0}};
    }

    // loop through all existing esdocks in increments of bulksize, then delete them
    let numDeleted = 0;
    for (let i = 0; i < Math.ceil(searchResponse.hits.total / this.bulkSize); i++) {
      const bulkDelete = [];
      const dumpDocs = searchResponse.hits.hits;
      for (let j = 0; j < dumpDocs.length; j++) {
        bulkDelete.push({
          delete: {
            _index: this.mappings[collectionName].index,
            _type: this.mappings[collectionName].type,
            _id: dumpDocs[j]._id
          }
        });
      }
      numDeleted += bulkDelete.length;
      console.log(`${collectionName} delete progress: ${numDeleted}/${searchResponse.hits.total}`);
      searchResponse = await this.esClient.scroll({
        scrollId: searchResponse._scroll_id,
        scroll: '1m'
      });
      await this.sendBulkRequest(bulkDelete);
    }
    return numDeleted;
  }

  setMappings(collection) {
    // set up mappings between mongo and elastic if they do not yet exist
    if (!this.mappings[collection]) this.mappings[collection] = {};
    if (!this.mappings[collection].index) {
      this.mappings[collection].index = this.mappings.default.index;
      if (this.mappings[collection].index === "$self")
        this.mappings[collection].index = collection;
    }
    if (!this.mappings[collection.type]) {
      this.mappings[collection].type = this.mappings.default.type;
      if (this.mappings[collection].type === "$self")
        this.mappings[collection].type = collection;
    }
  }


  sendBulkRequest(bulkOp) {
    if (bulkOp.length === 0) {
      return;
    }
    return this.esClient.bulk({
      refresh: false,
      body: bulkOp
    }).catch(err => {
      console.log(err);
    })
  }

}

module.exports = ElasticManager;
