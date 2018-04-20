const elasticsearch = require('elasticsearch');
const logger = new (require('service-logger'))(__filename);

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
      logger.info(`- ${change.documentKey._id.toString()}: ${change.ns.coll} ${change.operationType}`);
      return replicationFunctions[change.operationType].call(this, change);
    }
    else {
      logger.error(`REPLICATION ERROR: ${change.operationType} is not a supported function`);
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
          _id: esId,
          _parent: esReadyDoc[this.mappings[changeStreamObj.ns.coll].parentId]
        }
      });
    this.bulkOp.push(esReadyDoc);
  }

  async deleteDoc(changeStreamObj) {
    const esId = changeStreamObj.documentKey._id.toString(); // convert mongo ObjectId to string

    const parentId = await this.findParentId(this.mappings[changeStreamObj.ns.coll], esId).catch((err) => {
      logger.error(`error finding parentId in delete: ${err}`);
    });

    this.bulkOp.push({
      delete: {
        _index: this.mappings[changeStreamObj.ns.coll].index,
        _type: this.mappings[changeStreamObj.ns.coll].type,
        _id: esId,
        _parent: parentId
      }
    });
  }

//find a parent Id of a document by searching for the document and pulling that Id out.
//returns null if the collection is not a child collection or does not contain a document with the given Id.
  async findParentId(collection, childId){
    let doc = await this.esClient.search({
      index: collection.index,
      type: collection.type,
      q: `_id:${childId}`
    });
    try{
      return doc.responses[0].hits.hits[0]._parent
    } catch(err) {
      logger.error(`cannot find item of type ${collection.type} with id ${childId}`);
    }
    return null;
  }

  // delete all docs in ES before dumping the new docs into it
  async deleteElasticCollection(collectionName) {
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
            _id: dumpDocs[j]._id,
            _parent: dumpDocs[j]._parent
          }
        });
      }
      numDeleted += bulkDelete.length;
      logger.info(`${collectionName} delete progress: ${numDeleted}/${searchResponse.hits.total}`);
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
    if (!this.mappings[collection]) {
      this.mappings[collection] = {};
    }
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
    }).then(response => {
      if (!response.errors) { return; }

      response.items.forEach(item => {
        let erroredItem;
        if(item.delete && item.delete.error){
          erroredItem = item.delete
        } else if (item.index && item.index.error) {
          erroredItem = item.index
        }
        if(erroredItem){
          logger.error(`Bulk Request Error: ${JSON.stringify(erroredItem)}`);
          if(erroredItem.error.type === 'routing_missing_exception'){
            logger.debug('This is most likely due do to a missing child parent relationship in the config.  See default config file for reference.');
          }
        }
      });
    }).catch(err => {
      logger.error(`Bulk Error: ${err}`);
    });
  }

}

module.exports = ElasticManager;
