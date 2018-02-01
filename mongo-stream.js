const BSON = require('bson');
const bson = new BSON();
const elasticsearch = require('elasticsearch');
const fs = require('fs');
const MongoClient = require('mongodb').MongoClient;


class MongoStream {
  constructor(esClient, db, limit) {
    this.esClient = esClient;
    this.db = db;
    this.changeStreams = {};
    this.dumpLimit = limit;
  }

  // constructs and returns a new MongoStream
  static async init(options) {
    const client = await MongoClient.connect(options.url, options.mongoOpts);
    const db = client.db(options.db);
    const esClient = new elasticsearch.Client(options.elasticOpts);
    return new MongoStream(esClient, db, options.dumpLimit);
  }

  // filters through collections and executes the specified operation on them
  // @param opts: {
  //   filterArray: an array used to filter through mongo collections
  //   filterType: the type of filter to use with the given filterArray (inclusive or exclusive)
  //   operation: the operation to apply to the specified collections (dump, add, or remove)
  // }
  async filterAndExecute(opts) {
    // Filter
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

    // Execute
    const streamFunctions = {
      'dump': this.collectionDump,
      'add': this.addChangeStream,
      'remove': this.removeChangeStream
    };
    console.log(streamFunctions);
    if (streamFunctions.hasOwnProperty(opts.operation)) {
      console.log(`"${opts.operation}"`);
      const responses = {};
      for (let i = 0; i < filteredCollections.length; i++) {
        console.log(`--- ${filteredCollections[i]}: ${opts.operation} BEGIN ---`);
        responses[filteredCollections[i]] = await streamFunctions[opts.operation].call(this, filteredCollections[i])
          .catch(err => console.log(`${opts.operation} error`, err));
        console.log(`--- ${filteredCollections[i]}: ${opts.operation} END ---`);
      }
      return responses;
    }
    else return `URL ERROR: ${opts.operation} is not a supported function`;

  }

  // overwrites an entire elasticsearch collection with the current collection state in mongodb
  async collectionDump(collectionName) {
    // since we're dumping the collection, remove the resume token
    if(fs.existsSync(`./resumeTokens/${collectionName}`)) fs.unlink(`./resumeTokens/${collectionName}`);

    await this.deleteElasticCollection(collectionName);

    let requestCount = 0;
    const startDate = new Date();
    let limit = 100000;
    const cursor = this.db.collection(collectionName).find({}, { skip: 0, limit: limit });
    const count = await this.db.collection(collectionName).count();
    if (count < limit) { limit = count }
    let bulkOp = [];
    let nextObject;
    for (let i = 0; i < limit; i++) {
      if (bulkOp.length !== 0 && bulkOp.length % (this.dumpLimit * 2) === 0) {
        requestCount += (bulkOp.length/2);
        console.log(`${collectionName} request progress: ${requestCount}/${count}`);
        this.sendBulkRequest(bulkOp);
        bulkOp = [];
      }

      nextObject = await cursor.next().catch(err => console.log('next object error', err));
      if (nextObject === null) {
        console.log((new Date() - startDate) / 1000);
        break;
      }

      const _id = nextObject._id;
      delete nextObject._id;
      bulkOp.push({ index:  { _index: this.db.databaseName, _type: collectionName, _id: _id } });
      bulkOp.push(nextObject);
    }
    requestCount += (bulkOp.length/2);
    console.log(`${collectionName} FINAL request progress: ${requestCount}/${count}`);
    await this.sendBulkRequest(bulkOp); // last bits
    console.log('done');
    return true;
  }

  async addChangeStream(collectionName) {
    const resumeToken = MongoStream.parseResumeToken(collectionName);
    if (!resumeToken) await this.collectionDump(collectionName);
    if (this.changeStreams[collectionName]) {
      console.log('change stream already exists, removing...');
      await this.removeChangeStream(collectionName);
    }

    this.changeStreams[collectionName] = this.db.collection(collectionName).watch({resumeAfter: resumeToken});
    const mongoStream = this; // I'm bad at scope, needed access to 'this' in the below callback
    this.changeStreams[collectionName].on('change', (change) => {
      const b64String = bson.serialize(change._id).toString('base64');
      fs.writeFileSync(`./resumeTokens/${collectionName}`, b64String, 'base64');
      mongoStream.replicate(change);
    });
    return true;
  }

  async removeChangeStream(collectionName) {
    if (this.changeStreams[collectionName]) {
      await this.changeStreams[collectionName].close();
      delete this.changeStreams[collectionName];
    }

    this.changeStreams[collectionName] = null;
    return true;
  }

  // Calls the appropriate replication function based on the change object parsed from a change stream
  async replicate(change) {
    const replicationFunctions = {
      'insert': this.insertDoc,
      'update': this.updateDoc,
      'replace': this.replaceDoc,
      'delete': this.deleteDoc,
      'invalidate': this.invalidateDoc
    };
    if (replicationFunctions.hasOwnProperty(change.operationType)) {
      await replicationFunctions[change.operationType].call(this, change)
        .catch(err => console.log(`${change.operationType} error`, err));
      console.log(`- ${change.documentKey._id.toString()}: ${change.ns.coll} ${change.operationType}`);
    }
    else {
      console.log(`REPLICATION ERROR: ${change.operationType} is not a supported function`);
    }
  }

// insert event format https://docs.mongodb.com/manual/reference/change-events/#insert-event
  insertDoc(changeStreamObj) {
    const esIndex = changeStreamObj.ns.db;
    const esType = changeStreamObj.ns.coll;
    const esId = changeStreamObj.fullDocument._id.toString(); // convert mongo ObjectId to string
    delete changeStreamObj.fullDocument._id;
    const esReadyDoc = changeStreamObj.fullDocument;

    return this.esClient.create({
      index: esIndex,
      type: esType,
      id: esId,
      body: esReadyDoc
    });
  }

// lookup doc in ES, apply changes, index doc
// not the most efficient but until we need to optimize it
// this is the most straightforward
  updateDoc(changeStreamObj) {
    const esIndex = changeStreamObj.ns.db;
    const esType = changeStreamObj.ns.coll;
    const esId = changeStreamObj.documentKey._id.toString(); // convert mongo ObjectId to string
    const updatedFields = changeStreamObj.updateDescription.updatedFields;
    const removedFields = changeStreamObj.updateDescription.removedFields;

    return this.esClient.get({
      index: esIndex,
      type: esType,
      id: esId
    }).then(doc => {
      const source = doc._source;
      removedFields.forEach(field => {
        delete source[field];
      });
      const esReadyDoc = Object.assign(source, updatedFields);

      return this.esClient.index({
        index: esIndex,
        type: esType,
        id: esId,
        body: esReadyDoc
      });
    });
  }

  replaceDoc(changeStreamObj) {
    const esIndex = changeStreamObj.ns.db;
    const esType = changeStreamObj.ns.coll;
    const esId = changeStreamObj.fullDocument._id.toString(); // convert mongo ObjectId to string
    delete changeStreamObj.fullDocument._id;
    const esReadyDoc = changeStreamObj.fullDocument;

    return this.esClient.index({
      index: esIndex,
      type: esType,
      id: esId,
      body: esReadyDoc
    });
  }

  deleteDoc(changeStreamObj) {
    const esIndex = changeStreamObj.ns.db;
    const esType = changeStreamObj.ns.coll;
    const esId = changeStreamObj.documentKey._id.toString(); // convert mongo ObjectId to string

    return this.esClient.delete({
      index: esIndex,
      type: esType,
      id: esId
    });
  }

  invalidateDoc(changeStreamObj) {
    console.log('invalidate change received. The watched collection has been dropped or renamed. Stream closing...');
    // do something to handle a stream closing I guess...
  }


  // delete all docs in ES before dumping the new docs into it
  async deleteElasticCollection(collectionName) {
    let searchResponse;
    try {
      // First get a count for all ES docs of the specified type
      searchResponse = await this.esClient.search({
        index: this.db.databaseName,
        type: collectionName,
        size: this.dumpLimit,
        scroll: '1m'
      });
    }
    catch (err) {
      // if the search query failed, the index or type does not exist
      searchResponse = {hits: {total: 0}};
    }

    // loop through all existing esdocks in increments of bulksize, then delete them
    let numDeleted = 0;
    for (let i = 0; i < Math.ceil(searchResponse.hits.total / this.dumpLimit); i++) {
      const bulkDelete = [];
      const dumpDocs = searchResponse.hits.hits;
      for (let j = 0; j < dumpDocs.length; j++) {
        bulkDelete.push({
          delete: {
            _index: this.db.databaseName,
            _type: collectionName,
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

  sendBulkRequest(bulkOp) {
    return this.esClient.bulk({
      refresh: false,
      body: bulkOp
    }).catch(err => {
      console.log(err);
    })
  }

  static parseResumeToken(collection) {
    try {
      const base64Buffer = fs.readFileSync(`./resumeTokens/${collection}`);
      return bson.deserialize(base64Buffer);
    } catch (err) {
      return null;
    }
  }


}

module.exports = MongoStream;
