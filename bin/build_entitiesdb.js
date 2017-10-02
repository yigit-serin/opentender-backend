const Store = require('../lib/store.js');
const async = require('async');
const status = require('node-status');

const console = status.console();

const config = require('../config.js');

let store = new Store(config);
let status_suppliers = status.addItem('suppliers', {type: ['count']});
let status_buyers = status.addItem('buyers', {type: ['count']});
let status_tenders = status.addItem('tenders', {type: ['bar']});

let streamItems = (onItems, onEnd) => {
	let query = {match_all: {}};
	let pos = 0;
	store.Tender.stream(1000, query,
		(items, total, next) => {
			pos += items.length;
			onItems(items, pos, total, next);
		},
		(err) => {
			onEnd(err);
		});
};

function importBuyers(items, cb) {
	if (items.length === 0) {
		return cb();
	}
	let buyers = [];
	items.forEach(hit => {
		if (!hit._source) {
			console.log('transmission failed, hit without a _source', hit);
			return;
		}
		(hit._source.buyers || []).forEach(body => {
			let buyer = buyers.find(b => {
				return b.body.groupId === body.groupId;
			});
			if (!buyer) {
				buyer = {
					id: body.groupId,
					body: body,
					sources: []
				};
				buyers.push(buyer);
			}
			buyer.sources.push({tender: hit._source.id, country: hit._source.country, body: body});
		});
	});
	let ids = buyers.map(buyer => {
		return buyer.body.groupId;
	});
	store.Buyer.getByIds(ids, (err, result) => {
		if (err) return cb(err);
		let new_list = [];
		let update_hits = [];
		buyers.forEach(buyer => {
			let hit = result.hits.hits.find(h => {
				return buyer.body.groupId === h._source.body.groupId;
			});
			if (hit) {
				hit._source.sources = hit._source.sources.concat(buyer.sources);
				update_hits.push(hit);
			} else {
				new_list.push(buyer);
				status_buyers.inc();
			}
		});
		store.Buyer.bulk_update(update_hits, (err) => {
			if (err) return cb(err);
			store.Buyer.bulk_add(new_list, (err) => {
				if (err) return cb(err);
				cb();
			});
		});
	});

}

function importSuppliers(items, cb) {
	if (items.length === 0) {
		return cb();
	}
	let suppliers = [];
	items.forEach(hit => {
		if (!hit._source) {
			return;
		}
		(hit._source.lots || []).forEach(lot => {
			(lot.bids || []).forEach(bid => {
				(bid.bidders || []).forEach(body => {
					let supplier = suppliers.find(b => {
						return b.body.groupId === body.groupId;
					});
					if (!supplier) {
						supplier = {
							id: body.groupId,
							body: body,
							sources: []
						};
						suppliers.push(supplier);
					}
					supplier.sources.push({tender: hit._source.id, country: hit._source.country, body: body});
				});
			});
		});
	});
	let ids = suppliers.map(supplier => {
		return supplier.body.groupId;
	});
	store.Supplier.getByIds(ids, (err, result) => {
		if (err) return cb(err);
		let new_list = [];
		let update_hits = [];
		suppliers.forEach(supplier => {
			let hit = result.hits.hits.find(h => {
				return supplier.body.groupId === h._source.body.groupId;
			});
			if (hit) {
				hit._source.sources = hit._source.sources.concat(supplier.sources);
				update_hits.push(hit);
			} else {
				new_list.push(supplier);
				status_suppliers.inc();
			}
		});
		store.Supplier.bulk_update(update_hits, (err) => {
			if (err) return cb(err);
			store.Supplier.bulk_add(new_list, (err) => {
				if (err) return cb(err);
				cb();
			});
		});
	});
}

let importEntities = (cb) => {
	streamItems((items, pos, total, next) => {
		status_tenders.max = total;
		status_tenders.count = pos;
		importBuyers(items, (err) => {
			if (err) {
				return cb(err);
			}
			importSuppliers(items, (err) => {
				if (err) {
					return cb(err);
				}
				next();
			});
		});
	}, (err) => {
		cb(err);
	});
};


store.init((err) => {
	if (err) {
		return console.log(err);
	}
	store.Buyer.removeIndex((err) => {
		if (err) {
			return console.log(err);
		}
		store.Buyer.checkIndex(err => {
			if (err) {
				return console.log(err);
			}
			store.Supplier.removeIndex((err) => {
				if (err) {
					return console.log(err);
				}
				store.Supplier.checkIndex(err => {
					if (err) {
						return console.log(err);
					}
					status.start();
					importEntities(err => {
						if (err) {
							return console.log(err);
						}
						store.close(() => {
							status.stop();
							console.log('done');
						});
					});
				});
			});
		});
	});
});
