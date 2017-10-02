const elasticsearch = require('elasticsearch');
const async = require('async');
const IndexStore = require('./indexstore.js');
const Mapping = require('./mappings.js');

class Store {

	constructor(config) {
		let mapping = new Mapping(config);
		// https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html
		let client = new elasticsearch.Client({
			host: config.elastic.host + ':' + config.elastic.port,
			requestTimeout: 300000,
			log: [{
				type: 'stdio',
				levels: config.elastic.log || ['error']
			}]
		});
		this.PublicBody = new IndexStore(mapping.PUBLICBODY, client);
		this.Tender = new IndexStore(mapping.TENDER, client);
		this.Buyer = new IndexStore(mapping.BUYER, client);
		this.Supplier = new IndexStore(mapping.SUPPLIER, client);
		this.Company = new IndexStore(mapping.COMPANY, client);
		this.Authority = new IndexStore(mapping.AUTHORITY, client);
		this.client = client;
	}

	init(cb) {
		console.log('Open connection to elasticsearch');
		this.client.ping({
				requestTimeout: 1000 // ping usually has a 100ms timeout
			},
			(error) => {
				if (error) {
					console.log('elasticsearch could not be reached');
					return cb(error);
				}
				async.forEachSeries(
					[this.PublicBody, this.Tender, this.Company, this.Authority, this.Buyer, this.Supplier], (index, next) => {
						index.checkIndex(next);
					}, () => {
						console.log('Connection open');
						cb();
					});
			});
	}

	close(cb) {
		this.client.close();
		cb();
	}
}


module.exports = Store;
