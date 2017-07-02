const amqp = require('amqplib');

const AMQP_URL = process.env.AMQP_URL || 'amqp://localhost';
const JOB_QUEUE = 'jobs';

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


exports.sendJob = async function (job) {
    try {
        const ch = await exports.getChannel();
        ch.sendToQueue(JOB_QUEUE, Buffer.from(JSON.stringify(job)));
    }
    catch ({message}) {
        console.error('Error:',message);
    }
};