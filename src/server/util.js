/**
 * generates a random id
 * @return {String}
 */
exports.generateId = () => 
    crypto.randomBytes(12).toString('hex');