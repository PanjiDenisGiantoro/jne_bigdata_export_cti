const Redis = require('ioredis');

const config = {
    user: 'dbctc_v2',
    password: 'dbctc123',
    connectString: '10.8.2.48:1521/ctcv2db'
};

const config_jnebill = {
    user: 'JNEBILL',
    password: 'JNE98292092B5494083OK',
    connectString: '10.8.2.219:1521/JNEBILL'
};

const config_jnebilltraining = {
    user: 'JNEBILL',
    password: 'JNEBILL',
    connectString: '10.8.2.19:1522/JNEBILL'
};

const redis = new Redis();

module.exports = {
    config,
    config_jnebill,
    config_jnebilltraining,
    redis
};
