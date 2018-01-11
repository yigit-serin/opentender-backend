const async = require('async');

class IndexStore {

	constructor(index, client) {
		this.index = index;
		this.client = client;
	}

	removeIndex(cb) {
		console.log('IndexStore:', this.index.id, 'removing index');
		this.client.indices.delete({'index': this.index.id}, (err) => {
			if (err) {
				console.log('IndexStore:', this.index, 'indices.delete', err.msg || err.toString());
			}
			cb();
		});
	}

	checkIndex(cb) {
		this.client.indices.exists({'index': this.index.id}, (err, response) => {
			if (err) {
				console.log('IndexStore:', this.index.id, 'indices.exists', err.msg || err.toString());
			}
			if (!response) {
				console.log('IndexStore:', this.index.id, 'creating index');
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
							console.log('IndexStore:', this.index.id, 'indices.create', err.msg || err.toString());
							return cb(err);
						}
						console.log('IndexStore:', this.index.id, 'index created');
						setTimeout(cb, 2000); //index takes some time to create TODO: test until db creation failed or is completed
					}
				);
			} else {
				cb();
			}
		});
	}

	bulk_add(docs, cb) {
		if (docs.length === 0) {
			return cb();
		}
		let bulk_request = [];
		docs.forEach(doc => {
			bulk_request.push({create: {_index: this.index.id, _type: this.index.id, _id: doc.id}});
			bulk_request.push(doc);
		});
		this.client.bulk(
			{body: bulk_request}, (err, response) => {
				if (err) {
					console.log('IndexStore:', err);
					console.log('IndexStore:', this.index.id, 'doc.bulk_add', err.msg || err.toString());
					return cb(err);
				}
				cb(err, response);
			});
	}

	bulk_update(items, cb) {
		if (items.length === 0) {
			return cb();
		}
		let bulk_request = [];
		items.forEach(item => {
			bulk_request.push({index: {_index: this.index.id, _type: this.index.id, _id: item._source.id}});
			bulk_request.push(item._source);
		});
		this.client.bulk(
			{body: bulk_request}, (err, response) => {
				if (err) {
					console.log('IndexStore:', err);
					console.log('IndexStore:', this.index.id, 'doc.bulk_update', err.msg || err.toString());
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
				console.log('IndexStore:', err);
				console.log('IndexStore:', this.index.id, 'doc.add', err.msg || err.toString());
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

	getByIds(ids, cb) {
		if (ids.length > 1000) {
			let queue = [];
			while (ids.length > 0) {
				let q = ids.slice(0, 1000);
				ids = ids.slice(1000);
				queue.push(q);
			}
			let result = null;
			async.forEachSeries(queue, (q, next) => {
				this.getByIds(q, (err, res) => {
					if (err) {
						return cb(err)
					}
					if (!result) {
						result = res;
					} else {
						result.hits.hits = result.hits.hits.concat(res.hits.hits);
					}
					next();
				});
			}, (err) => {
				if (err) {
					return cb(err)
				}
				cb(err, result);
			});
			return;
		}
		let q = {};
		q[this.index.id_field] = ids;
		this.client.search({
			index: this.index.id,
			type: this.index.id,
			size: ids.length,
			body: {
				query: {
					terms: q
				}
			}
		}, cb);
	}

	store(body, cb) {
		this.client.index({
			index: this.index.id,
			type: this.index.id,
			body: body
		}, cb);
	}

	update(id, body, cb) {
		this.client.index({
			index: this.index.id,
			type: this.index.id,
			id: id,
			body: body
		}, cb);
	}

	search(body, size, from, cb) {
		// console.log('IndexStore:','search:\n', JSON.stringify(body, null, '\t'));
		this.client.search({
			index: this.index.id,
			type: this.index.id,
			size: size,
			from: from,
			body: body
		}, cb);
	}

	streamQuery(size, query, mapper, cb) {

		let getMoreUntilDone = (err, response) => {
			if (err) {
				return cb(err);
			}
			if (response.hits.hits.length === 0) {
				return cb();
			}
			if (!mapper(response.hits.hits, response.hits.total)) {
				return cb('aborted');
			}
			// now we can call scroll over and over
			setTimeout(() => {
				this.client.scroll({
					scrollId: response._scroll_id,
					scroll: '10s'
				}, getMoreUntilDone);
			}, 100);
		};

		this.client.search({
			index: this.index.id,
			type: this.index.id,
			size: size,
			scroll: '10s',
			body: {
				query: query
			}
		}, getMoreUntilDone);
	}

	stream(size, query, onItems, onEnd) {

		let getMoreUntilDone = (err, response) => {
			if (err) {
				return onEnd(err);
			}
			if (response.hits.hits.length === 0) {
				return onEnd();
			}
			// now we can call scroll over and over
			onItems(response.hits.hits, response.hits.total, () => {
				setTimeout(() => {
					this.client.scroll({
						scrollId: response._scroll_id,
						scroll: '10m'
					}, getMoreUntilDone);
				}, 100);
			});
		};

		this.client.search({
			index: this.index.id,
			type: this.index.id,
			size: size,
			scroll: '10m',
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
							'ot.country': country_id
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
