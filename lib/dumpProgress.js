const logger = new (require('service-logger'))(__filename);
const fs = require('fs');
const ObjectId = require('mongodb').ObjectId;


class DumpProgress {
  constructor(collection) {
    this.collection = collection;
    this.token = null;
    this.count = null;
    this.startCount = 0;
    this.completeDate = null;
  }


  async get() {
    if (!this.token) {
      if (DumpProgress.storageCollection) {
        await this.getFromCollection();
      } else {
        this.getFromFile();
      }
    }
  }


  async getFromCollection() {
    try {
      const {token, count, completeDate} = await DumpProgress.storageCollection.findOne({ _id: this.collection });
      this.token = ObjectId(token);
      this.count = count;
      this.startCount = count;
      this.completeDate = completeDate;
    } catch (err) {
      logger.err(`dumpProgress for ${this.collection} could not be retrieved from database`);
      logger.debug(err);
      this.reset();
    }
  }

  getFromFile() {
    try {
      const {token, count, completeDate} = JSON.parse(fs.readFileSync(`./dumpProgress/${this.collection}`, 'utf8'));
      this.token = ObjectId(token);
      this.count = count;
      this.startCount = count;
      this.completeDate = completeDate;
    } catch (err) {
      this.reset();
    }
  }

  async write() {
    if (DumpProgress.storageCollection) {
      await this.writeToCollection();
    } else {
      this.writeToFile();
    }
  }


  writeToFile() {
    fs.writeFileSync(`./dumpProgress/${this.collection}`, JSON.stringify({token: this.token, count: this.count, completeDate: this.completeDate}));
    logger.debug(`dumpProgress for collection ${this.collection} saved to disk`);
  }

  async writeToCollection() {
    try {
      await DumpProgress.storageCollection.updateOne(
        { _id: this.collection },
        { $set: { token: this.token, count: this.count, completeDate: this.completeDate }},
        { upsert: true },
      );
      logger.debug(`resumeToken for collection ${this.collection} saved to database`);
    } catch (err) {
      logger.err(`resumeToken for collection ${this.collection} could not be saved to database`);
      logger.debug(err);
    }
  }

  async notComplete() {
    this.completeDate = null;
    await this.write();
  }

  async complete() {
    this.completeDate = new Date();
    await this.write();
  }

  async reset() {
    this.token = ObjectId('000000000000000000000000');
    this.count = 0;
    this.completeDate = null;
    await this.write();
  }

}

module.exports = DumpProgress;
