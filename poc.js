const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
const BSON = require('bson')
const bson = new BSON();
const url = 'mongodb://localhost:27017';
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
MongoClient.connect(url, (err, client) => {
  const db = client.db(dbName);
  const stream = db.collection(collectionName).watch({ resumeAfter: resumeToken });

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
