const BSON = require('bson');
const bson = new BSON();
const fs = require('fs');

class ChangeStream {
  constructor(db, collection) {
    this.collection = collection;
    this.resumeToken = this.getResumeToken();
    this.changeStream = db.collection(collection).watch({resumeAfter: this.resumeToken, fullDocument: 'updateLookup'});
  }

  listen(elasticManager) {
    this.changeStream.on('change', (change) => {
      this.resumeToken = change._id;
      if (change.operationType === 'invalidate') this.remove();
      else elasticManager.replicate(change);
    });
  }

  async remove() {
    await this.changeStream.close();
    delete this.changeStream;
    this.resumeToken = null;
    this.collection = null;
  }

  getResumeToken() {
    if (!this.resumeToken) {
      try {
        const base64Buffer = fs.readFileSync(`./resumeTokens/${this.collection}`);
        this.resumeToken = bson.deserialize(base64Buffer);
      } catch (err) {
        this.resumeToken = null;
      }
    }

    return this.resumeToken;
  }

  writeResumeToken() {
    if (!this.resumeToken) return;
    const b64String = bson.serialize(this.resumeToken).toString('base64');
    fs.writeFileSync(`./resumeTokens/${this.collection}`, b64String, 'base64');
  }

  removeResumeToken() {
    this.resumeToken = null;
    if(fs.existsSync(`./resumeTokens/${this.collection}`)) fs.unlink(`./resumeTokens/${this.collection}`);
  }

}

module.exports = ChangeStream;
