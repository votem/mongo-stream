const elasticsearch = require('elasticsearch');
const logger = new (require('service-logger'))(__filename);
const versioning = require('./versioning');
const jsonpatch = require('json-patch');
const _ = require('lodash');

class ElasticManager {
  constructor(elasticOpts, mappings, bulkSize) {
    this.esClient = new elasticsearch.Client(elasticOpts);
    this.mappings = mappings;
    this.bulkSize = bulkSize;
    this.bulkOp = [];
    this.interval = null;
    this.esMappings = {};
    this.esFields = {};
  }

/*
* Note to future Nick, I have no idea where I left off in this commit but I've got to save this and move on.
* TODO: Figure out why I wrote seperate "fields" and "mappings" functions, shouldn't they always be loaded at the same time?
*       It looks like I'm calling pieces of them seperately but no idea why. Also loadESAll seems to be redundant with
*       setMappings. One is probably a newer way of doing it that I was working on but I can't remember which.
*/

  async loadESAll() {
    this.esFields = {};
    this.esMappings = {};
    this.mappings = {};

    await this.loadESFields();
    await this.loadESMappings();

    _.forEach(this.esMappings, (val, index) => {
      _.forEach(val.mappings, (mapObject, collection) => {

        const mapObject = _.get(this.esFields, `${index}.mappings.${collection}`, {});
        const fields =  _.filter(_.keys(mapObject), (k) => {return !k.match(/(keyword|phonetic|raw)$/) && !k.match(/^_/)});

        const parent =  _.get(esMappings, `${index}.mappings.${collection}._parent.type`);
        const parentProp = _.get(esMappings, `${index}.mappings.${collection}._parent.type.properties.${parent}Id`);

        this.mappings[collection] = {
          index: index,
          type: collection,
          fields: fields,
          parent: (parent && parentProp) ? parent : null,
          parentId: (parent && parentProp) ? `${parent}Id` : null
        };
      });
    });

    return this.mappings;
  }

  loadESFields() {
    if (!_.isEmpty(this.esFields)) {
      return Promise.resolve(this.esFields);
    }

    return this.esClient.indices.getFieldMapping({fields: '*'}).then((rez) => {
      this.esFields = rez;
      logger.info('ES fields retrieved');
      return this.esFields;
    }).catch((err) => {
      logger.error(`Error Connecting to ES to get fields: `+ err);
      return this.esFields;
    });
  }


  //Gets elasticsearch mappings for the purposes of initilizing any parentId's
  loadESMappings() {
    if (!_.isEmpty(this.esMappings)) {
      return Promise.resolve(this.esMappings);
    }

    return this.esClient.indices.getMapping().then((rez) => {
      this.esMappings = rez;
      logger.info('ES mappings retrieved');
      return this.esMappings;
    }).catch((err) => {
      logger.error(`Error Connecting to ES using ${elasticOpts} to get mappings: `+ err);
      return this.esMappings;
    });
  }

  getCollections() {
    return _.keys(this.mappings);
  }

  async setMappings(collection) {
    // set up mappings between mongo and elastic if they do not yet exist
    if (!this.mappings[collection]) {
      this.mappings[collection] = {};
    }
    if (!this.mappings[collection].index) {
      this.mappings[collection].index = (this.mappings.default.index === "$self") ? collection : this.mappings.default.index;
    }
    if (!this.mappings[collection].type) {
      this.mappings[collection].type = (this.mappings.default.type === "$self") ? collection : this.mappings.default.type;
    }
    if (this.mappings[collection].transformations) {
      this.mappings[collection].transformFunc = jsonpatch.compile(this.mappings[collection].transformations);
    }

    // Tries to guess the parentId field name based on parent type, can be explicitly overriden by specifying 'parent' & 'parentId' in the config json loaded on startup
    // Note: 'parentId' property name should really be 'parentIdField' but keeping it as is for backwards-compatability
    if (!this.mappings[collection].parent) {
      const esMappings = await this.loadESMappings();
      const parent =  _.get(esMappings, `${this.mappings[collection].index}.mappings.${this.mappings[collection].type}._parent.type`);
      const parentProp = _.get(esMappings, `${this.mappings[collection].index}.mappings.${this.mappings[collection].type}.properties.${parent}Id`);

      if (parent && parentProp) {
        this.mappings[collection].parent = parent;
        this.mappings[collection].parentId = `${parent}Id`;
      }
    }

    if (!this.mappings[collection].fields) {
      const esFields = await this.loadESFields();
      const mapObject = _.get(esFields, `${this.mappings[collection].index}.mappings.${this.mappings[collection].type}`, {});

      this.mappings[collection].fields =  _.filter(_.keys(mapObject), (k) => {return !k.match(/(keyword|phonetic|raw)$/) && !k.match(/^_/)});
    }
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
    if (changeStreamObj.fullDocument === null) return;
    const esId = changeStreamObj.fullDocument._id.toString(); // convert mongo ObjectId to string
    delete changeStreamObj.fullDocument._id;
    const esReadyDoc = changeStreamObj.fullDocument;
    const mapObject = this.mappings[changeStreamObj.ns.coll];

    if (mapObject.parentId && !esReadyDoc[mapObject.parentId]) {
      logger.info(`Skipping insert of ${changeStreamObj.ns.coll}:${esId} -- parentId('${mapObject.parentId}') specified but could not find one in record`)
      return;
    }

    this.bulkOp.push({
        index:  {
          _index: mapObject.index,
          _type: mapObject.type,
          _id: esId,
          _parent: esReadyDoc[mapObject.parentId],
          _versionType: mapObject.versionType,
          _version: versioning.getVersionAsInteger(esReadyDoc[mapObject.versionField])
        }
      });

    const transformedDoc = this.transformDoc(changeStreamObj.ns.coll, esReadyDoc);

    this.bulkOp.push(transformedDoc);
  }

  transformDoc(collName, esReadyDoc) {
    const transformFunc = this.mappings[collName].transformFunc;
    const transformations = this.mappings[collName].transformations;

    // Filters out any properties in the inserted object not in esMappings.
    // TODO: This should probably be condfigurable
    if (!_.isEmpty(this.mappings[collName].fields)) {
      esReadyDoc = _.pick(esReadyDoc, this.mappings[collName].fields);
    }

    if(transformFunc) {
      return transformFunc(esReadyDoc);
    }
    else if(transformations) {
      return jsonpatch.apply(esReadyDoc, transformations);
    }
    else {
      return esReadyDoc;
    }
  }

  async deleteDoc(changeStreamObj) {
    const esId = changeStreamObj.documentKey._id.toString(); // convert mongo ObjectId to string
    const mapObject = this.mappings[changeStreamObj.ns.coll];

    const { parentId, version } = await this.getExistingDoc(mapObject, esId).catch((err) => {
      logger.error(`error finding existing document in delete: ${err}`);
     });

    this.bulkOp.push({
      delete: {
        _index: mapObject.index,
        _type: mapObject.type,
        _id: esId,
        _parent: parentId,
        _versionType: mapObject.versionType,
        _version: versioning.incrementVersionForDeletion(version),
      }
    });
  }

  async getExistingDoc(collection, id) {
    try {
      const doc = await this.esClient.search({
        index: collection.index,
        type: collection.type,
        q: `_id:${id}`,
        size: 1,
        version: Boolean(collection.versionType),
      });

      return {
        parentId: doc.hits.hits[0]._parent || null,
        version: doc.hits.hits[0]._version || null,
      }
    } catch(err) {
      logger.error(`cannot find item of type ${collection.type} with id ${id}`);
    }
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
        scroll: '1m',
        version: Boolean(this.mappings[collectionName].versionType)
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
      const mapObject = this.mappings[collectionName];

      for (let j = 0; j < dumpDocs.length; j++) {
        bulkDelete.push({
          delete: {
            _index: mapObject.index,
            _type: mapObject.type,
            _id: dumpDocs[j]._id,
            _parent: dumpDocs[j]._parent,
            _versionType: mapObject.versionType,
            _version: versioning.incrementVersionForDeletion(dumpDocs[j]._version)
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
        if (item.delete && item.delete.error) {
          erroredItem = item.delete
        } else if (item.index && item.index.error) {
          erroredItem = item.index
        }

        if (erroredItem) {
          logger.error(`Bulk Request Error:`, erroredItem);
          if (erroredItem.error.type === 'routing_missing_exception') {
            logger.debug('This is most likely due do to a missing child parent relationship in the config.  See default config file for reference.');
          }
        }
      });
    }).catch(err => {
      logger.error(`Bulk Error:`, err);
    });
  }

}

module.exports = ElasticManager;
