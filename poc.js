const MongoClient = require('mongodb').MongoClient;
const elasticsearch = require('elasticsearch');
const esClient = new elasticsearch.Client({ host: 'localhost:9200', apiVersion: '2.4' });
const fs = require('fs');
const f = require('util').format;
const BSON = require('bson')
const bson = new BSON();
let url = process.env.MONGO_RS || 'mongodb://localhost:27017';
const dbName = 'vrs';
const collectionName = 'voter';
let resumeToken;
const ARG_SKIP = Number(process.argv[2]) || 0;
const ARG_LIMIT = Number(process.argv[3]) || 100000;
const ARG_DUMP = process.argv[4] === '-d' ? true : false;
const BULK_SIZE = 1000;
let bulkOpsDone = 0;
const testDoc = require('./testDoc');


try {
  const base64Buffer = fs.readFileSync('./resumeToken');
  resumeToken = bson.deserialize(base64Buffer);
} catch (err) {
  console.log(err)
  resumeToken = null;
}
console.log('resumeToken', resumeToken);

let options = {};

if (process.env.ROOT_FILE_PATH) {
  const ca = fs.readFileSync(process.env.ROOT_FILE_PATH);
  const cert = fs.readFileSync(process.env.KEY_FILE_PATH);
  const key = fs.readFileSync(process.env.KEY_FILE_PATH);

  options = {
    ssl: true,
    sslCA: ca,
    sslKey: key,
    sslCert: cert
  };

  const user = encodeURIComponent(process.env.MONGO_USER);
  url = f('mongodb://%s@%s', user, process.env.MONGO_RS)
}

console.log('connecting to mongo...');
console.log('url', url);
MongoClient.connect(url, options, (err, client) => {
  if (err)
    console.error(err);

  const db = client.db(dbName);
  if (ARG_DUMP) {
    collectionDump(db, collectionName);
  };
  const stream = db.collection(collectionName).watch({ resumeAfter: resumeToken });

  console.log('connected');

  db.collection(collectionName).insert(testDoc);
  stream.on('change', function(change) {
    console.log("a change is here\n", change)
    const b64String = bson.serialize(change._id).toString('base64');
    fs.writeFileSync('./resumeToken', b64String, 'base64');
    replicate(change);
  });
});

async function replicate(change) {
  switch (change.operationType) {
    case 'insert':
      await insertDoc(change).catch(err => console.log(`${change.operationType} error`, err));
      console.log('done');
      break;
    case 'update':
      updateDoc(change).catch(err => console.log(`${change.operationType} error`, err));
      console.log('done');
      break;
    case 'replace':
      replaceDoc(change).catch(err => console.log(`${change.operationType} error`, err));
      console.log('done');
      break;
    case 'delete':
      deleteDoc(change).catch(err => console.log(`${change.operationType} error`, err));
      console.log('done');
      break;
    case 'invalidate':
      invalidate(change).catch(err => console.log(`${change.operationType} error`, err));
      console.log('done');
      break;
    default:
      console.log(change.operationType, 'not supported yet');
  }
}

// insert event format https://docs.mongodb.com/manual/reference/change-events/#insert-event
function insertDoc(changeStreamObj) {
  console.log('inserting');
  const esIndex = changeStreamObj.ns.db;
  const esType = changeStreamObj.ns.coll;
  const esId = changeStreamObj.fullDocument._id.toString(); // convert mongo ObjectId to string
  delete changeStreamObj.fullDocument._id;
  const esReadyDoc = changeStreamObj.fullDocument;

  return esClient.create({
    index: esIndex,
    type: esType,
    id: esId,
    body: esReadyDoc
  });
}

// lookup doc in ES, apply changes, index doc
// not the most efficient but until we need to optimize it
// this is the most straightforward
function updateDoc(changeStreamObj) {
  console.log('updating');
  const esIndex = changeStreamObj.ns.db;
  const esType = changeStreamObj.ns.coll;
  const esId = changeStreamObj.documentKey._id.toString(); // convert mongo ObjectId to string
  const updatedFields = changeStreamObj.updateDescription.updatedFields;
  const removedFields = changeStreamObj.updateDescription.removedFields

  return esClient.get({
    index: esIndex,
    type: esType,
    id: esId
  }).then(doc => {
    const source = doc._source;
    removedFields.forEach(field => {
      delete source.field;
    });
    const esReadyDoc = Object.assign(source, updatedFields);

    return esClient.index({
      index: esIndex,
      type: esType,
      id: esId,
      body: esReadyDoc
    });
  });
}

function replaceDoc(changeStreamObj) {
  console.log('replacing');
  const esIndex = changeStreamObj.ns.db;
  const esType = changeStreamObj.ns.coll;
  const esId = changeStreamObj.fullDocument._id.toString(); // convert mongo ObjectId to string
  delete changeStreamObj.fullDocument._id;
  const esReadyDoc = changeStreamObj.fullDocument;

  return esClient.index({
    index: esIndex,
    type: esType,
    id: esId,
    body: esReadyDoc
  });
}

function deleteDoc(changeStreamObj) {
  console.log('deleting');
  const esIndex = changeStreamObj.ns.db;
  const esType = changeStreamObj.ns.coll;
  const esId = changeStreamObj.documentKey._id.toString(); // convert mongo ObjectId to string

  return esClient.delete({
    index: esIndex,
    type: esType,
    id: esId
  });
}

function invalidate(changeStreamObj) {
  console.log('invalidate change received. The watched collection has been dropped or renamed. Stream closing...');
  // do something to handle a stream closing I guess...
}

async function collectionDump(db, collectionName) {
  const cursor = db.collection(collectionName).find({}, { skip: ARG_SKIP, limit: ARG_LIMIT });
  const count = db.collection(collectionName).count();
  if (count < ARG_LIMIT) { ARG_LIMIT = count }
  let bulkOp = [];
  let nextObject;
  for (var i = 0; i < ARG_LIMIT; i++) {
    if (bulkOp.length !== 0 && bulkOp.length % (BULK_SIZE * 2) === 0) {
      sendBulkrequest(bulkOp);
      console.log(bulkOpsDone);
      bulkOp = [];
    }

    nextObject = await cursor.next().catch(err => console.log('next object error', err));
    const _id = nextObject._id;
    delete nextObject._id;
    bulkOp.push({ index:  { _index: dbName, _type: collectionName, _id: _id } });
    bulkOp.push(nextObject);
  }
  await sendBulkrequest(bulkOp); // last bits
  console.log('done')
  process.exit();
}

function sendBulkrequest(bulkOp) {
  return esClient.bulk({
    refresh: false,
    body: bulkOp
  }).then(resp => {
    bulkOpsDone++;
  }).catch(err => {
    console.log(err);
  })
}
