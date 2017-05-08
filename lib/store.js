const elasticsearch = require('elasticsearch');
const async = require('async');
const IndexStore = require('./indexstore.js');
const Mapping = require('./mappings.js');

const Store = function (config) {
	let me = this;
	let mapping = new Mapping(config);

	// https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html
	let client = new elasticsearch.Client({
		host: config.elastic.host + ':' + config.elastic.port,
		requestTimeout: 30000 * 10,
		log: [{
			type: 'stdio',
			levels: config.elastic.log || ['error']
		}]
	});

	me.PublicBody = new IndexStore(mapping.PUBLICBODY, client);
	me.Tender = new IndexStore(mapping.TENDER, client);
	me.Company = new IndexStore(mapping.COMPANY, client);
	me.Authority = new IndexStore(mapping.AUTHORITY, client);

	me.init = function (cb) {
		console.log('Open connection to elasticsearch');
		client.ping({
			requestTimeout: 1000 // ping usually has a 100ms timeout
		}, (error) => {
			if (error) {
				console.log('elasticsearch could not be reached');
				return cb(error);
			}
			async.forEachSeries(
				[me.PublicBody, me.Tender, me.Company, me.Authority], (index, next) => {
					index.checkIndex(next);
				}, () => {
					console.log('Connection open');
					cb();
				});
		});
	};

	me.close = function (cb) {
		client.close();
		cb();
	};

};

module.exports = Store;