const BSON = require('bson');
const bson = new BSON();
const elasticsearch = require('elasticsearch');
const fs = require('fs');
const MongoClient = require('mongodb').MongoClient;


class MongoStream {
  constructor(esClient, db) {
    this.esClient = esClient;
    this.db = db;
    this.changeStreams = {};
  }

  // constructs and returns a new MongoStream
  static async init() {
    const { url, mongoOpts } = MongoStream.setMongoOpts();
    const dbName = process.env.MONGO_DB;
    const elasticOpts = {
      host: process.env.ELASTIC_HOST,
      apiVersion: process.env.ELASTIC_API_VER
    };

    const client = await MongoClient.connect(url, mongoOpts);
    const db = client.db(dbName);
    const esClient = new elasticsearch.Client(elasticOpts);
    const mongoStream = new MongoStream(esClient, db);

    // parse inclusive/exclusive collection list, set up security before adding a changestream
    let collectionSecurity;
    if (process.env.COLL_INCLUSIVE) {
      const inclusiveCollections = JSON.parse(process.env.COLL_INCLUSIVE);
      collectionSecurity = function(collectionName) {
        return inclusiveCollections.indexOf(collectionName) > -1;
      }
    }
    if (process.env.COLL_EXCLUSIVE) {
      const exclusiveCollections = JSON.parse(process.env.COLL_EXCLUSIVE);
      collectionSecurity = function(collectionName) {
        return exclusiveCollections.indexOf(collectionName) === -1;
      }
    }

    const collections = await db.collections();
    for (let i = 0; i < collections.length; i++) {
      const collectionName = collections[i].collectionName;
      if (!collectionSecurity(collectionName)) continue;
      mongoStream.addChangeStream(collectionName);
    }

    return mongoStream;
  }

  static setMongoOpts() {
    let url = process.env.MONGO_RS;
    let mongoOpts = {};

    // if MongoDB requires SSL connection, configure options here
    if (process.env.ROOT_FILE_PATH) {
      const ca = fs.readFileSync(process.env.ROOT_FILE_PATH);
      const cert = fs.readFileSync(process.env.KEY_FILE_PATH);
      const key = fs.readFileSync(process.env.KEY_FILE_PATH);

      mongoOpts = {
        ssl: true,
        sslCA: ca,
        sslKey: key,
        sslCert: cert
      };

      const user = encodeURIComponent(process.env.MONGO_USER);
      url = f('mongodb://%s@%s', user, process.env.MONGO_RS)
    }

    return {url, mongoOpts};
  }

  // delete all docs in ES before dumping the new docs into it
  // There's a better way to do this, I'm sure, but I'll figure it out later
  async deleteESCollection(collectionName, limit) {
    let allESDocks;
    try {
      // First get a count for all ES docs of the specified type
      allESDocks = await this.esClient.count({
        index: this.db.databaseName,
        type: collectionName
      });
    }
    catch(err) {
      // if the count query failed, the index or type does not exist
      allESDocks = {count: 0}
    }

    // loop through all existing esdocks in increments of bulksize, then delete them
    let numDeleted = 0;
    for (let i = 0; i < Math.ceil(allESDocks.count / limit); i++) {
      const searchResponse = await this.esClient.search({
        index: this.db.databaseName,
        type: collectionName,
        from: limit * i,
        size: limit
      });
      const bulkDelete = [];
      const dumpDocs = searchResponse.hits.hits;
      for (let j = 0; j < dumpDocs.length; j++) {
        bulkDelete.push({delete: {_index: this.db.databaseName, _type: collectionName, _id: dumpDocs[j]._id}})
      }
      numDeleted += bulkDelete.length;
      console.log(`${collectionName} delete progress: ${numDeleted}/${allESDocks.count}`);
      await this.sendBulkRequest(bulkDelete);
    }

    return numDeleted;
  }

  // overwrites an entire elasticsearch collection with the current collection state in mongodb
  async collectionDump(collectionName) {
    console.log(`dumping from ${collectionName}`);
    const limit = process.env.BULK_LIMIT;

    await this.deleteESCollection(collectionName, limit);

    // count and replicate documents from mongo into elasticsearch
    const count = await this.db.collection(collectionName).count();
    let bulkOpsDone = 0;
    for (let i = 0; i < Math.ceil(count / limit); i++) {
      const docPack = await this.db.collection(collectionName).find({}, {
        limit: limit,
        skip: i * limit
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