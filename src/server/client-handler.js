const amqp = require('./amqp');


/**
 * handles clients of the specified websocket server
 */
module.exports = class ClientHandler {

    /**
     * @param {WebSocket.Server} wss websocket server whose clients to handle
     * @param {string} serverId id of the server
     */
    constructor (wss, serverId) {
        this.wss = wss;
        this.serverId = serverId;
        this._initHandler();
    }

    /**
     * initializes event handlers for
     * the websocket server and client messages
     */
    _initHandler () {
        this.wss.on('connection', ws => {
            ws.on('message', data => {
                const msg = JSON.parse(data);
                this._onMessage(msg);
            });
            // identify server to connected client
            this.send(ws, this._createIdMessage());
        });
    }

    /**
     * handles incoming message from client
     * @param {object} msg decoded message from client
     */
    _onMessage (msg) {
        switch (msg.type) {
            case 'id':
                return this._handleIdMessage(msg);
            case 'job':
                return this._handleJobMessage(msg);
            default:
                console.warn("Unhandled message", msg);
        }
    }

    /**
     * handles id message from client
     * @param {object} msg 
     */
    _handleIdMessage (msg) {
         console.log('Client connected:', msg.clientId);
    }

    /**
     * handles job message from client
     * @param {object} msg 
     */
    _handleJobMessage (msg) {
        amqp.sendJob(Object.assign(msg, 
            { serverId: this.serverId }));
    }

    /**
     * message used to identify the server instance
     * @return {object}
     */
    _createIdMessage () {
        return {
            type: 'id',
            serverId: this.serverId
        };
    }

    /**
     * sends message to specified client
     * @param {WebSocket} ws websocket connection to client
     * @param {object} msg 
     */
    send (ws, msg) {
        ws.send(JSON.stringify(msg));
    }

    /**
     * sends message to all clients
     * @param {object} msg 
     */
    broadcast (msg) {
        this.wss.clients.forEach(ws => {
            this.send(ws, msg);
        });
    }
};