const { randomBytes } = require('crypto');

/**
 * generates a random id
 * @return {String}
 */
exports.generateId = () => 
    randomBytes(12).toString('hex');