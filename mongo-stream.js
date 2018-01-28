const BSON = require('bson');
const bson = new BSON();
const elasticsearch = require('elasticsearch');
const fs = require('fs');
const MongoClient = require('mongodb').MongoClient;


class MongoStream {
    constructor(db){
        this.esClient =  new elasticsearch.Client({host: 'localhost:9200', apiVersion: '2.4'});
        this.db = db;
    }

    static async init(url, options, dbName) {
        const client = await MongoClient.connect(url, options);
        const db = client.db(dbName);
        const mongoStream = new MongoStream(db);
        const collections = await db.collections();
        for (let i = 0; i < collections.length; i++) {
            const collectionName = collections[i].collectionName;
            if (collectionName !== 'voter') continue;
            console.log(collectionName);
            const resumeToken = mongoStream.parseResumeToken(collectionName);
            if (!resumeToken) await mongoStream.collectionDump(collectionName);
            const change = db.collection(collectionName).watch({resumeAfter: resumeToken});
            change.on('change', function (change) {
                const b64String = bson.serialize(change._id).toString('base64');
                fs.writeFileSync(`./resumeTokens/${collectionName}`, b64String, 'base64');
                mongoStream.replicate(change);
            });
        }

        return mongoStream;
    }

    async collectionDump(collectionName) {
        console.log(`dumping from ${collectionName}`);
        this.esClient.indices.delete({index: collectionName});
        const count = await this.db.collection(collectionName).count();
        console.log(count);
        let bulkOpsDone = 0;

        const limit = 100;
        for (let i = 0; i < Math.ceil(count/limit); i++) {
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
            bulkOpsDone += bulkOp.length/2;
            await this.sendBulkrequest(bulkOp);
            console.log(`${collectionName}: ${bulkOpsDone}/${count}`);
        }

        console.log('done');

        return bulkOpsDone;
    }

    sendBulkrequest(bulkOp) {
        return this.esClient.bulk({
            refresh: false,
            body: bulkOp
        }).then(resp => {
            return;
        }).catch(err => {
            console.log(err);
        })
    }

    parseResumeToken(collection) {
        try {
            const base64Buffer = fs.readFileSync(`./resumeTokens/${collection}`);
            return bson.deserialize(base64Buffer);
        } catch (err) {
            console.log(err.message);
            return null;
        }
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