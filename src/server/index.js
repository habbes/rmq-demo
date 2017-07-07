const http = require('http');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const WebSocket = require('ws');
const amqp = require('./amqp');
const { generateId } = require('./util');
const ClientHandler = require('./client-handler');

const PORT = process.env.PORT || 3000;
const SERVER_ID = generateId();

// Run web server to server static html page

const server = http.createServer((req, res) => {
    const stream = fs.createReadStream(path.join(__dirname, '../client/index.html'));
    res.writeHead(200, {'content-type': 'text/html'});
    stream.pipe(res);
});

server.listen(PORT, () => {
    console.log("Server %s listening on port %d", SERVER_ID, PORT);
});

// WebSocket server

const wss = new WebSocket.Server({ server });
const clientHandler = new ClientHandler(wss, SERVER_ID);

// RabbitMQ event handler

amqp.subscribe((msg) => {
    switch (msg.type) {
        case 'result':
            return clientHandler.broadcast(msg);
        default:
            console.log('Unhandled event', msg);
    }
});
