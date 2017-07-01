const http = require('http');
const path = require('path');
const fs = require('fs');
const WebSocket = require('ws');
const PORT = process.env.PORT || 3000;

// Run web server to server static html page

const server = http.createServer((req, res) => {
    const stream = fs.createReadStream(path.join(__dirname, '../web/index.html'));
    res.writeHead(200, {'content-type': 'text/html'});
    stream.pipe(res);
});

server.listen(PORT, () => {
    console.log("Server listening on", PORT);
});

// WebSocket server

const wss = new WebSocket.Server({ server });
wss.on('connection', conn => {
    console.log('client connected');
    conn.on('message', data => {
        console.log('message received %j', data);
    });
});