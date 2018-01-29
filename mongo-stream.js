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
    const mongoStream = new MongoStream(esClient, db, options.dumpLimit);

    let filterFunction;
    if (options.coll_inclusive) {
      filterFunction = function (collectionName) {
        return options.coll_inclusive.indexOf(collectionName) > -1;
      }
    }
    else if (options.coll_exclusive) {
      filterFunction = function (collectionName) {
        return options.coll_exclusive.indexOf(collectionName) === -1;
      }
    }
    else {
      // if inclusive/exclusive is not declared, no change streams are added, they must be added manually
      filterFunction = function (collectionName) {
        return false;
      }
    }

    const collections = await db.collections();
    for (let i = 0; i < collections.length; i++) {
      const collectionName = collections[i].collectionName;
      if (!filterFunction(collectionName)) continue;
      mongoStream.addChangeStream(collectionName);
    }

    return mongoStream;
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

  // overwrites an entire elasticsearch collection with the current collection state in mongodb
  async collectionDump(collectionName) {
    console.log(`dumping from ${collectionName}`);

    // since we're dumping the collection, remove the resume token
    if(fs.existsSync(`./resumeTokens/${collectionName}`)) fs.unlink(`./resumeTokens/${collectionName}`);

    await this.deleteElasticCollection(collectionName);

    // count and replicate documents from mongo into elasticsearch
    const count = await this.db.collection(collectionName).count();
    let bulkOpsDone = 0;
    for (let i = 0; i < Math.ceil(count / this.dumpLimit); i++) {
      const docPack = await this.db.collection(collectionName).find({}, {
        limit: this.dumpLimit,
        skip: i * this.dumpLimit
      }).toArray();
      const bulkOp = [];
      for (let j = 0; j < docPack.length; j++) {
        const _id = docPack[j]._id;
        delete docPack[j]._id;
        bulkOp.push({index: {_index: this.db.databaseName, _type: collectionName, _id: _id}});
        bulkOp.push(docPack[j]);
      }
      bulkOpsDone += bulkOp.length / 2;
      await this.sendBulkRequest(bulkOp);
      console.log(`${collectionName}s replicated: ${bulkOpsDone}/${count}`);
    }

    console.log('done');
    return bulkOpsDone;
  }

  sendBulkRequest(bulkOp) {
    return this.esClient.bulk({
      refresh: false,
      body: bulkOp
    }).then(resp => {
      return;
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

  async addChangeStream(collectionName) {
    const resumeToken = MongoStream.parseResumeToken(collectionName);
    if (!resumeToken) await this.collectionDump(collectionName);
    if (this.changeStreams[collectionName]) {
      console.log('change stream already exists, removing...');
      this.removeChangeStream(collectionName);
    }
    this.changeStreams[collectionName] = this.db.collection(collectionName).watch({resumeAfter: resumeToken});
    const mongoStream = this; // I'm bad at scope, needed access to 'this' in the below callback
    this.changeStreams[collectionName].on('change', function (change) {
      const b64String = bson.serialize(change._id).toString('base64');
      fs.writeFileSync(`./resumeTokens/${collectionName}`, b64String, 'base64');
      mongoStream.replicate(change);
    });

    this.changeStreams[collectionName] = this.db.collection(collectionName).watch();
  }

  removeChangeStream(collectionName) {
    if (this.changeStreams[collectionName])
      this.changeStreams[collectionName].close();

    this.changeStreams[collectionName] = null;
  }

  async replicate(change) {
    console.log(`${change.documentKey._id.toString()} - ${change.ns.coll} ${change.operationType} BEGIN`);
    await this[`${change.operationType}Doc`](change)
      .catch(err => console.log(`${change.operationType} error`, err));
    console.log(`${change.documentKey._id.toString()} - ${change.ns.coll} ${change.operationType} END`);
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

}

module.exports = MongoStream;
