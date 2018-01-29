const express = require('express');
const app = express();
const port = 8421;

const MongoStream = require('./mongo-stream');
let mongoStream;

app.get('/', (request, response) => {
    response.send('Hello from Express!')
});

// triggers a manual collection dump for the specified collection
app.get('/dump/:collection', (request, response) => {
    console.log(`dumping collection ${request.params.collection} to ES`);
    mongoStream.collectionDump(request.params.collection);
    response.send(`dumping collection ${request.params.collection} to ES`);
});

// returns the status of all change streams currently running
app.get('/monitor', (request, response) => {
    const changeStreams = Object.keys(mongoStream.changeStreams);
    const responseBody = {};
    for (let i = 0; i < changeStreams.length; i++) {
        if (mongoStream.changeStreams[changeStreams[i]]) {
            responseBody[changeStreams[i]] = 'Listening'
        }
        else {
            responseBody[changeStreams[i]] = 'Not Listening'
        }
    }

    response.send(JSON.stringify(responseBody, null, 2));
});

// starts listening to the change stream of the specified collection
app.get('/start/:collection', (request, response) => {
    mongoStream.addChangeStream(request.params.collection).then(() => {
      response.send(`${request.params.collection} change stream added`);
    });
});

// stops listening to the change stream of the specified collection
app.get('/stop/:collection', (request, response) => {
    mongoStream.removeChangeStream(request.params.collection);
    response.send(`${request.params.collection} change stream removed`);
});

app.listen(port, (err) => {
    if (err) {
        return console.log(`Error listening on ${port}: `, err)
    }

    MongoStream.init('mongodb://localhost:27017', {}, 'vrs', {host: 'localhost:9200', apiVersion: '2.4'}).then((stream) => {
        console.log('connected');
        mongoStream = stream;
    });
    console.log(`server is listening on port ${port}`);
});