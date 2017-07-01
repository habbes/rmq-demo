const http = require('http');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const WebSocket = require('ws');
const PORT = process.env.PORT || 3000;
const SERVER_ID = generateServerId();

// Run web server to server static html page

const server = http.createServer((req, res) => {
    const stream = fs.createReadStream(path.join(__dirname, '../web/index.html'));
    res.writeHead(200, {'content-type': 'text/html'});
    stream.pipe(res);
});

server.listen(PORT, () => {
    console.log("Server %s listening on port %d", SERVER_ID, PORT);
});

// WebSocket server

const wss = new WebSocket.Server({ server });
wss.on('connection', conn => {
    conn.on('message', data => {
        const msg = JSON.parse(data);
        handleMessage(msg);
    });
    sendMessage(conn, buildIdMessage(SERVER_ID));
});


/**
 * generates a random id to assign to server instance
 * @return {String}
 */
function generateServerId () {
    return crypto.randomBytes(12).toString('hex');
}

// CLIENT MESSAGES

function sendMessage (conn, msg) {
    conn.send(JSON.stringify(msg));
}

/**
 * message used to identify the server instance
 * @param {String} id unique server id
 * @return {Object}
 */
function buildIdMessage (id) {
    return {
        type: 'id',
        serverId: id
    };
}

// Message handlers

/**
 * handles a message from a client
 * based on its type
 * @param {Object} msg 
 */
function handleMessage (msg) {
    switch (msg.type) {
        case 'id':
            return handleIdMessage(msg);
        default:
            console.warn("Unhandled message", msg);
    }
}

/**
 * handles and id message from a client
 * @param {Object} msg 
 */
function handleIdMessage (msg) {
    console.log('Client connected:', msg.clientId);
}