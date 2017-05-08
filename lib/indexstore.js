const debug = require('debug')('portals:store');

const IndexStore = function (index, client) {
	let me = this;

	me.removeIndex = function (cb) {
		console.log(index.id, 'removing index');
		client.indices.delete({'index': index.id}, (err) => {
			if (err) {
				console.log(index, 'indices.delete', err.msg || err.toString());
			}
			cb();
		});
	};

	me.checkIndex = function (cb) {
		client.indices.exists({'index': index.id}, function (err, response) {
			if (err) {
				console.log(index.id, 'indices.exists', err.msg || err.toString());
			}
			if (!response) {
				console.log(index.id, 'creating index');
				client.indices.create(
					{
						index: index.id,
						ignore_conflicts: false,
						body: {mappings: index.mapping, settings: index.settings}
					},
					(err) => {
						if (err) {
							console.log(index.id, 'indices.create', err.msg || err.toString());
							return cb(err);
						}
						console.log(index.id, 'index created');
						setTimeout(cb, 2000); //index takes some time to create TODO: test until db creation failed or is completed
					}
				);
			} else {
				cb();
			}
		});
	};

	me.bulk_add = function (docs, cb) {
		let bulk_request = [];
		docs.forEach(doc => {
			bulk_request.push({create: {_index: index.id, _type: index.id, _id: doc.id}});
			bulk_request.push(doc);
		});
		client.bulk(
			{
				body: bulk_request
			}, function (err, response) {
				if (err) {
					console.log(err);
					console.log(index.id, 'doc.bulk', err.msg || err.toString());
					return cb(err);
				}
				cb(err, response);
			});
	};

	me.add = function (doc, cb) {
		client.index({
			index: index.id,
			type: index.id,
			requestTimeout: 300000,
			body: doc
		}, function (err, response) {
			if (err) {
				console.log(err);
				console.log(index.id, 'doc.add', err.msg || err.toString());
				return cb(err);
			}
			cb(err, response);
		});
	};

	me.get = function (id, cb) {
		let match = {};
		match[index.id_field] = id;
		client.search({
			index: index.id,
			type: index.id,
			size: 1,
			body: {"query": {"match": match}}
		}, cb);
	};

	me.search = function (body, size, from, cb) {
		client.search({
			index: index.id,
			type: index.id,
			size: size,
			from: from,
			body: body
		}, cb);
	};

	me.all = function (cb) {
		let body = {
			'query': {
				'match_all': {}
			}
		};
		//TODO: replace max 10000 with streaming
		me.search(body, 10000, 0, (error, response) => {
			if (error) return cb(error);
			let result = [];
			if (response) {
				result = response.hits.hits.map((hit) => {
					return hit._source;
				});
			}
			cb(null, result);
		});
	};

	me.stream = function (mapper, cb) {
		me.streamQuery({match_all: {}}, mapper, cb);
	};

	me.streamQuery = function (query, mapper, cb) {
		let count = 0;
		client.search({
			index: index.id,
			type: index.id,
			scroll: '10s',
			body: {
				query: query
			}
		}, function getMoreUntilDone(err, response) {
			if (err) {
				return cb(err);
			}
			response.hits.hits.forEach((item) => {
				mapper(item, ++count, response.hits.total);
			});
			if (response.hits.total !== count) {
				// now we can call scroll over and over
				client.scroll({
					scrollId: response._scroll_id,
					scroll: '10s'
				}, getMoreUntilDone);
			} else {
				cb();
			}
		});
	};

	me.aggregations = function (country_id, aggregations, cb) {
		let body = {
			'query': {
				'match_all': {}
			},
			'aggregations': aggregations
		};
		if (country_id) {
			body.query = {
				term: {
					'country': country_id
				}
			};
		}
		me.search(body, 0, 0, (error, response) => {
			if (error) return cb(error);
			cb(null, response);
		});
	};

};

module.exports = IndexStore;