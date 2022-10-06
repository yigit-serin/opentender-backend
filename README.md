# Opentender Portal Data Backend

Provides the Data for the Opentender Portal

written in Javascript for NodeJS 8.x & Elasticsearch 2.4.6

## Installation

- install [NodeJS](https://nodejs.org/) 8.x and [NPM](https://www.npmjs.com/)
- install [Elasticsearch](https://www.elastic.co/) 2.4.6
- install [Elasticsearch OptenTender Plugin](https://github.com/digiwhist/elasticsearch-native-script-opentender) 2.4.6

- run command `npm install` in the root folder of this repository

- prepare the data folder (see https://github.com/opentender-jm/opentender-data)

- copy file 'config.dist.js' to 'config.js' and make the changes to reflect your infrastructure

```javascript
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
```

## Commands

### Start

`npm run server` to run the server

### Develop

`npm run develop` to run the server & reload on file changes

### Check

`npm run check` to check the tender api data and the transformation according to the schema.json files

### Import

`npm run import` to import the tender api data into the DB

### Stopwords

`npm run stopwords` to create a joined stopword list file for the DB

(Note: elastic search only uses this file on import, so data must be re-imported after updating)

### Downloads

`npm run downloads` to create the country specific download package files from DB
