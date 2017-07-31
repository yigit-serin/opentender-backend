class IndexStore {

	constructor(index, client) {
		this.index = index;
		this.client = client;
	}

	removeIndex(cb) {
		console.log(this.index.id, 'removing index');
		this.client.indices.delete({'index': this.index.id}, (err) => {
			if (err) {
				console.log(this.index, 'indices.delete', err.msg || err.toString());
			}
			cb();
		});
	}

	checkIndex(cb) {
		this.client.indices.exists({'index': this.index.id}, (err, response) => {
			if (err) {
				console.log(this.index.id, 'indices.exists', err.msg || err.toString());
			}
			if (!response) {
				console.log(this.index.id, 'creating index');
				this.client.indices.create(
					{
						index: this.index.id,
						ignore_conflicts: false,
						body: {
							mappings: this.index.mapping,
							settings: this.index.settings
						}
					},
					(err) => {
						if (err) {
							console.log(this.index.id, 'indices.create', err.msg || err.toString());
							return cb(err);
						}
						console.log(this.index.id, 'index created');
						setTimeout(cb, 2000); //index takes some time to create TODO: test until db creation failed or is completed
					}
				);
			} else {
				cb();
			}
		});
	}

	bulk_add(docs, cb) {
		let bulk_request = [];
		docs.forEach(doc => {
			bulk_request.push({create: {_index: this.index.id, _type: this.index.id, _id: doc.id}});
			bulk_request.push(doc);
		});
		this.client.bulk(
			{body: bulk_request}, (err, response) => {
				if (err) {
					console.log(err);
					console.log(this.index.id, 'doc.bulk', err.msg || err.toString());
					return cb(err);
				}
				cb(err, response);
			});
	}

	add(doc, cb) {
		this.client.index({
			index: this.index.id,
			type: this.index.id,
			requestTimeout: 300000,
			body: doc
		}, (err, response) => {
			if (err) {
				console.log(err);
				console.log(this.index.id, 'doc.add', err.msg || err.toString());
				return cb(err);
			}
			cb(err, response);
		});
	}

	get(id, cb) {
		let match = {};
		match[this.index.id_field] = id;
		this.client.search({
			index: this.index.id,
			type: this.index.id,
			size: 1,
			body: {
				query: {
					match: match
				}
			}
		}, cb);
	}

	search(body, size, from, cb) {
		// console.log('search:\n', JSON.stringify(body, null, '\t'));
		this.client.search({
			index: this.index.id,
			type: this.index.id,
			size: size,
			from: from,
			body: body
		}, cb);
	}

	all(cb) {
		let body = {
			query: {
				match_all: {}
			}
		};
		//TODO: replace max 10000 with streaming
		this.search(body, 10000, 0, (error, response) => {
			if (error) {
				return cb(error);
			}
			let result = [];
			if (response) {
				result = response.hits.hits.map((hit) => {
					return hit._source;
				});
			}
			cb(null, result);
		});
	}

	stream(mapper, cb) {
		this.streamQuery({match_all: {}}, mapper, cb);
	};

	streamQuery(query, mapper, cb) {
		let count = 0;

		let getMoreUntilDone = (err, response) => {
			if (err) {
				return cb(err);
			}
			response.hits.hits.forEach((item) => {
				mapper(item, ++count, response.hits.total);
			});
			if (response.hits.total !== count) {
				// now we can call scroll over and over
				this.client.scroll({
					scrollId: response._scroll_id,
					scroll: '10s'
				}, getMoreUntilDone);
			} else {
				cb();
			}
		};

		this.client.search({
			index: this.index.id,
			type: this.index.id,
			scroll: '10s',
			body: {
				query: query
			}
		}, getMoreUntilDone);
	}

	aggregations(country_id, aggregations, cb) {
		let body = {
			query: {
				match_all: {}
			},
			aggregations: aggregations
		};
		if (country_id) {
			body.query = {
				bool: {
					filter: [{
						term: {
							'country': country_id
						}
					}]
				}
			};
		}
		this.search(body, 0, 0, (error, response) => {
			if (error) {
				return cb(error);
			}
			cb(null, response);
		});
	}
}

module.exports = IndexStore;
