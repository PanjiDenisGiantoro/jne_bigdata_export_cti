const Redis = require('ioredis');
require('dotenv').config();

// Main Database Configuration
const config = {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    connectString: `${process.env.DB_HOST}:${process.env.DB_PORT}/${process.env.DB_SERVICE_NAME}`
};

// JNE Bill Database Configuration
const config_jnebill = {
    user: process.env.JNEBILL_USER,
    password: process.env.JNEBILL_PASSWORD,
    connectString: `${process.env.JNEBILL_HOST}:${process.env.JNEBILL_PORT}/${process.env.JNEBILL_SERVICE_NAME}`
};

// JNE Bill Training Database Configuration
const config_jnebilltraining = {
    user: process.env.JNEBILL_TRAINING_USER,
    password: process.env.JNEBILL_TRAINING_PASSWORD,
    connectString: `${process.env.JNEBILL_TRAINING_HOST}:${process.env.JNEBILL_TRAINING_PORT}/${process.env.JNEBILL_TRAINING_SERVICE_NAME}`
};

// Initialize Redis client
const redis = new Redis();

// Log database connection status (only in development)
if (process.env.NODE_ENV === 'production') {
    console.log('Database Configuration:');
    console.log('- Main DB:', config.connectString);
    console.log('- JNE Bill DB:', config_jnebill.connectString);
    console.log('- JNE Bill Training DB:', config_jnebilltraining.connectString);
}

module.exports = {
    config,
    config_jnebill,
    config_jnebilltraining,
    redis
};
