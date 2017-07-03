const amqp = require('amqplib');

const AMQP_URL = process.env.AMQP_URL || 'amqp://localhost';
const JOB_QUEUE = 'jobs';
const EVENT_EX = 'events';

// reuse channel
let channel;

/**
 * creates a connection and channel to 
 * rabbitmq
 * @return {Promise} resolves with a channel instance
 */
exports.getChannel = async function () {
    if (channel) return channel;
    const conn = await amqp.connect(AMQP_URL);
    channel = await conn.createChannel();
    channel.assertQueue(JOB_QUEUE, {durable:true});
    return channel;
};

/**
 * send msg to job queue
 * @param {Object} job
 */
exports.sendJob = async function (job) {
    try {
        const ch = await exports.getChannel();
        ch.sendToQueue(JOB_QUEUE, Buffer.from(JSON.stringify(job)));
    }
    catch ({message}) {
        console.error('Error:',message);
    }
};

/**
 * register handler to incoming events
 * @param {Function} handler
 */
exports.subscribe = async function (handler) {
    const ch = await exports.getChannel();
    ch.assertExchange(EVENT_EX, 'fanout', {durable: false});
    const q = ch.assertQueue('', {exclusive: true});
    ch.bindQueue(q.queue, EVENT_EX, '');
    ch.consume(q.queue, msg => {
        const ev = JSON.parse(msg.content.toString());
        console.log('Event received %j', ev);
        handler(ev);
    }, {noAck: true});
};