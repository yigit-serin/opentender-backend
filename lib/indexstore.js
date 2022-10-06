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
				console.log('IndexStore removeIndex:', this.index, 'indices.delete', err.msg || err.toString());
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
                this.client.indices.create({
                    index: this.index.id,
                    body: {
                        settings: this.index.settings,
                        mappings: this.index.mapping
                    }
                }, (error, response) => {
                    if (error) {
                        console.log('IndexStore:', this.index.id, 'indices.create', error.msg || error.toString());

                        return cb(error);
                    } else {
                        console.log('IndexStore:', this.index.id, 'index created');
                        setTimeout(cb, 2000); //index takes some time to create TODO: test until db creation failed or is completed
                    }
                });
            } else {
                cb()
            }
        });
    }

	bulk_add(docs, cb) {
		if (docs.length === 0) {
			return cb();
		}
        const bulk_request = docs.flatMap(doc => [{ index: { _index: this.index.id } }, doc])
		this.client.bulk(
            {body: bulk_request}, (error, response) => {
                if (response.errors) {
                    console.log('IndexStore:', this.index.id, 'doc.bulk_add', 'not a critical errors when importing data:');

                    response.items.forEach(item => {
                        if (item.index.status !== 201) {
                            console.log('  Type:', item.index.error.type);
                            console.log('    Message error:', item.index.error.reason);
                        }
                    })
                }

                if (error) {
                    console.log('IndexStore:', this.index.id, 'doc.bulk_add', error.msg || error.toString());
                    console.log('IndexStore:', error);

                    return cb(error);
                }
                cb(error, response);
            });
    }

	bulk_update(items, cb) {
		if (items.length === 0) {
			return cb();
		}
        const bulk_request = items.flatMap(doc => [{ index: { _index: this.index.id } }, doc])
		this.client.bulk(
            {body: bulk_request}, {}, (error, response) => {
                if (response.errors) {
                    console.log('IndexStore:', this.index.id, 'doc.bulk_update', 'not a critical errors when importing data:');

                    response.items.forEach(item => {
                        if (item.index.status !== 201) {
                            console.log('  Type:', item.index.error.type);
                            console.log('    Message error:', item.index.error.reason);
                        }
                    })
                }

                if (error) {
                    console.log('IndexStore:', this.index.id, 'doc.bulk_update', error.msg || error.toString());
                    console.log('IndexStore:', error);

                    return cb(error);
                }
                cb(error, response);
            });
    }

	add(doc, cb) {
		this.client.index({
			index: this.index.id,
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
			size: 1,
			body: {
				query: {
					match
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
			body: body
		}, cb);
	}

	update(id, body, cb) {
		this.client.index({
			index: this.index.id,
			id: id,
			body: body
		}, cb);
	}

	search(body, size, from, cb) {
		// console.log('IndexStore:', `search ${this.index.id}:\n` , JSON.stringify(body, null, '\t'));
		this.client.search({
			index: this.index.id,
            body,
            track_total_hits: true,
            from: from,
		}, cb);
	}

	streamQuery(size, query, mapper, cb, isGetAllData) {

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

		if (isGetAllData) {
			this.client.search({
				index: this.index.id,
				size: size,
				scroll: '10s'
			}, getMoreUntilDone);
		} else {
			this.client.search({
				index: this.index.id,
				size: size,
				scroll: '10s',
				body: {
					query: query
				}
			}, getMoreUntilDone);
		}
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
