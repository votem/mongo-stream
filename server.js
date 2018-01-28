const express = require('express');
const app = express();
const port = 8421;

// const poc = require('./poc');
const MongoStream = require('./mongo-stream');
let mongoStream;

app.get('/', (request, response) => {
    response.send('Hello from Express!')
});

app.get('/dump/:collection', (request, response) => {
    console.log(`dumping collection ${request.params.collection} to ES`);
    mongoStream.collectionDump(request.params.collection);
    response.send(`dumping collection ${request.params.collection} to ES`);
});

app.listen(port, (err) => {
    if (err) {
        return console.log(`Error listening on ${port}: `, err)
    }

    MongoStream.init('mongodb://localhost:27017', {}, 'vrs').then((stream) => {
        console.log('connected');
        mongoStream = stream;
    });
    console.log(`server is listening on port ${port}`);
});