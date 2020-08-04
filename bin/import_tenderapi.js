#!/usr/bin/env node

/*

 Import tender api packages into the DB

 - clears the DB
 - checks tenderapi packages whith JSON Schema
 - stores Tender into DB tender
 - stores Tender.buyers into DB authority
 - stores Tender.lots.bids.bidders into DB company

 */

const path = require('path');
const fs = require('fs');
const async = require('async');
const Ajv = require('ajv');
const status = require('node-status');
const console = status.console();

let status_tenders = status.addItem('tenders', {type: ['count']});
let status_suppliers = status.addItem('suppliers', {type: ['count']});
let status_buyers = status.addItem('buyers', {type: ['count']});

const Store = require('../lib/store.js');
const Library = require('../lib/library.js');
const Converter = require('../lib/convert.js');

const config = require('../config.js');
const data_path = config.data.tenderapi;
const store = new Store(config);
const library = new Library(config);
const converter = new Converter(null, library, config.data.path);
let tender_count = 0;
let stats = {};

const validator = (filename) => {
	const ajv = new Ajv({verbose: true, jsonPointers: true, allErrors: true});
	const schema = JSON.parse(fs.readFileSync(path.resolve(config.data.shared, filename)).toString());
	return ajv.compile(schema);
};

const validateOpentender = validator('schema.json');
const validateTenderAPI = validator('tenderapi.json');

let clearIndex = (index, cb) => {
	async.waterfall([
		(next) => index.removeIndex(next),
		(next) => index.checkIndex(next),
	], (err) => {
		cb(err);
	});
};

let openDB = (cb) => {
	async.waterfall([
		(next) => store.init(next),
		(next) => clearIndex(store.Tender, next),
		(next) => clearIndex(store.Buyer, next),
		(next) => clearIndex(store.Supplier, next),
	], (err) => {
		cb(err);
	});
};

let safeBulkPackages = (array) => {
	let bulk_packages = [];
	while (array.length > 0) {
		let bulk_package = array.slice(0, 1000);
		bulk_packages.push(bulk_package);
		array = array.slice(1000);
	}
	return bulk_packages;
};

let importBulk = (array, index, status, cb) => {
	if (array.length === 0) {
		return cb();
	}
	let bulk_packages = safeBulkPackages(array);
	async.forEachSeries(bulk_packages, (bulk_package, next) => {
		index.bulk_add(bulk_package, (err) => {
			if (!err) {
				status.inc(bulk_package.length);
			}
			next(err);
		});
	}, (err) => {
		cb(err);
	});
};

let updateBulk = (array, index, cb) => {
	if (array.length === 0) {
		return cb();
	}
	let bulk_packages = safeBulkPackages(array);
	async.forEachSeries(bulk_packages, (bulk_package, next) => {
		index.bulk_update(bulk_package, (err) => {
			next(err);
		});
	}, (err) => {
		cb(err);
	});
};

let importTenderPackage = (array, filename, cb) => {
	let valid = validateTenderAPI(array);
	if (!valid) {
		return cb({msg: 'tenderapi schema error in filename ' + filename, errors: validateTenderAPI.errors});
	}

	array = converter.transform(array);

	valid = validateOpentender(array);
	if (!valid) {
		return cb({msg: 'opentender schema error in filename ' + filename, errors: validateOpentender.errors});
	}

	array.forEach(item => {
		stats[item.country] = (stats[item.country] || 0) + 1;
	});

	async.waterfall([
		(next) => importBulk(array, store.Tender, status_tenders, next),
		(next) => importBuyers(array, next),
		(next) => importSuppliers(array, next)
	], (err) => {
		cb(err);
	});
};

let importTenderPackageFile = (filename, cb) => {
	// read & decompress a package file
	let fullfilename = path.join(data_path, 'import', filename);
	fs.readFile(fullfilename, (err, content) => {
		if (err) {
			return cb(err);
		}
		console.log('Importing', fullfilename);
		let array = JSON.parse(content.toString());
		importTenderPackage(array, filename, cb);
	});
};

let importBuyers = (items, cb) => {
	if (items.length === 0) {
		return cb();
	}
	let buyers = [];
	items.forEach(item => {
		(item.buyers || []).forEach(body => {
			body.id = body.id || 'no-id';
			let buyer = buyers.find(b => {
				return b.body.id === body.id;
			});
			if (!buyer) {
				buyer = {
					id: body.id,
					body: body,
					countries: [],
					count: 0
				};
				buyers.push(buyer);
			}
			if (buyer.countries.indexOf(item.ot.country) < 0) {
				buyer.countries.push(item.ot.country);
			}
			buyer.count++;
		});
	});
	let ids = buyers.map(buyer => {
		return buyer.body.id;
	});
	store.Buyer.getByIds(ids, (err, result) => {
		if (err) return cb(err);
		let new_list = [];
		let update_hits = [];
		buyers.forEach(buyer => {
			let hit = result.hits.hits.find(h => {
				return buyer.body.id === h._source.body.id;
			});
			if (hit) {
				update_hits.push(hit);
			} else {
				new_list.push(buyer);
			}
		});
		updateBulk(update_hits, store.Buyer, (err) => {
			if (err) return cb(err);
			importBulk(new_list, store.Buyer, status_buyers, (err) => {
				cb(err);
			});
		});
	});

};

let importSuppliers = (items, cb) => {
	if (items.length === 0) {
		return cb();
	}
	let suppliers = [];
	items.forEach(item => {
		(item.lots || []).forEach(lot => {
			(lot.bids || []).forEach(bid => {
				(bid.bidders || []).forEach(body => {
					body.id = body.id || 'no-id';
					let supplier = suppliers.find(b => {
						return b.body.id === body.id;
					});
					if (!supplier) {
						supplier = {
							id: body.id,
							body: body,
							count: 0,
							countries: []
						};
						suppliers.push(supplier);
					}
					supplier.count++;
					if (supplier.countries.indexOf(item.ot.country) < 0) {
						supplier.countries.push(item.ot.country);
					}
				});
			});
		});
	});
	let ids = suppliers.map(supplier => {
		return supplier.body.id;
	});
	store.Supplier.getByIds(ids, (err, result) => {
		if (err) return cb(err);
		let new_list = [];
		let update_hits = [];
		suppliers.forEach(supplier => {
			let hit = result.hits.hits.find(h => {
				return supplier.body.id === h._source.body.id;
			});
			if (hit) {
				update_hits.push(hit);
			} else {
				new_list.push(supplier);
			}
		});
		updateBulk(update_hits, store.Supplier, (err) => {
			if (err) return cb(err);
			importBulk(new_list, store.Supplier, status_suppliers, (err) => {
				if (err) return cb(err);
				cb();
			});
		});
	});
};

let importTenderPackageFiles = (filename, cb) => {
	openDB((err) => {
		if (err) {
			return cb(err);
		}
		status.start();
		importTenderPackageFile(filename, err => {
			status.stop();
			if (tender_count > 0) {
				console.log('Tender Country Stats:', JSON.stringify(stats));
			} else {
				console.error('Could not read any tenders');
				store.close(() => {
					cb(err);
				});
			}
		});
	});
};

importTenderPackageFiles('JM-dataset.json', err => {
	if (err) {
		console.log(err);
	}
	if (!err) {
		console.log('done.');
	}
});
