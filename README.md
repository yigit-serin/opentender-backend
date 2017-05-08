# Opentender Portal Data Backend

Provides the Data for the Opentender Portal Frontend

written in Javascript for NodeJS 6.x

## Installation

- install NodeJS 6.x and NPM (TODO: URL)
- install Elasticsearch 2.4.x (TODO: URL)

- run command `npm install` in the root folder of this repository

- prepare the data folder (see digiwhist/data-folder)

- copy file 'config.dist.js' to 'config.js' and make the changes to reflect your infrastructure

```javascript
let settings = {
	listen: {  // where the backend should be listening
		host: '127.0.0.1',
		port: 3001
	},
	elastic: { // where elastic search is listening
		host: '127.0.0.1',
		port: 9200,
		log: ['info', 'error']
	},
	data: { // absolute paths to the data folders (see digiwhist/data-folder)
		shared: '/var/www/opentender/data/shared',
		path: '/var/www/opentender/data/backend',
		tenderapi: '/var/www/opentender/data/tenderapi'
	},
	disableCache: false // json is cached, disable here for debugging purposes
};
```

## Commands

### Start

`npm run server` to run the server

### Develop

`npm run develop` to run the server & reload on file changes

### Import

`npm run import` to import the tender api data into the DB

### Stopwords

`npm run stopwords` to create a joined stopword list file for the DB

(Note: elastic search only uses this file on import, so data must be reimported after updating)

### Downloads

`npm run downloads` to create the country specific download package files from DB

## Uses

* Node 6.x
* Elasticsearch 2.4