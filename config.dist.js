const dotenv = require('dotenv');
const result = dotenv.config({path: __dirname+'/.env'});

if (result.error) {
    throw result.error;
}

const { parsed: envs } = result;

console.log(envs);

const envMapping = {
    listen: {
        // where the backend should be listening
        host: process.env.SERVER_HOST,
        port: Number.parseInt(process.env.SERVER_PORT),
    },
    elastic: {
        // where elastic search is listening
        host: process.env.ELASTIC_HOST,
        port: Number.parseInt(process.env.ELASTIC_PORT),
        log: String(process.env.ELASTIC_LOG).split(','),
    },
    data: { // absolute paths to the data folders (see https://github.com/opentender-ug/opentender-data)
        shared: '../data/shared',
        path: '../data/backend',
        tenderapi: '../data/tenderapi'
    },
    cache: {
        type: process.env.CACHE_TYPE, // disabled | internal | memcached
        memcached: String(process.env.CACHE_MEMCACHED).split(',') // if type == memcached, server address(es)
    },
    country: {
        code: process.env.COUNTRY_CODE
    }
};
module.exports = envMapping;
