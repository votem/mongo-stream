const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
const f = require('util').format;
const BSON = require('bson')
const bson = new BSON();
let url = process.env.MONGO_RS || 'mongodb://localhost:27017';
const dbName = 'vrs';
let resumeToken;

try {
  const base64Buffer = fs.readFileSync('./resumeToken');
  console.log(base64Buffer);
  resumeToken = bson.deserialize(base64Buffer);
  console.log(resumeToken);
} catch (err) {
  console.log(err)
  resumeToken = null;
}
console.log('resumeToken', resumeToken);

const collectionName = 'voter';

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
  const stream = db.collection(collectionName).watch({ resumeAfter: resumeToken });

  console.log('connected');

  stream.on('change', function(change) {
    console.log(change)
    const b64String = bson.serialize(change._id).toString('base64');
    fs.writeFileSync('./resumeToken', b64String, 'base64');
    replicate(change);
  });
});

replicate = (change) => {
  switch (change.operationType) {
    case 'insert':
      console.log('inserting');
      break;
    default:
      console.log(change.operationType, 'not supported yet');
  }
}
