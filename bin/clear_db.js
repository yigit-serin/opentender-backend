#!/usr/bin/env node

/*

 Clears the DB, no questions asked

 */

const async = require('async');
const Store = require('../lib/store.js');
const config = require('../config.js');

let store = new Store(config);
store.init((err) => {
	if (err) {
		return console.log(err);
	}
	async.waterfall([
		(next) => store.Tender.removeIndex(next),
		(next) => store.Supplier.removeIndex(next),
		(next) => store.Buyer.removeIndex(next),
	], (err) => {
		if (err) return console.log(err);
		console.log('done.');
	});
});
