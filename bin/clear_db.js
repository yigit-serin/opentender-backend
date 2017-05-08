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
		store.Tender.removeIndex,
		store.Authority.removeIndex,
		store.Company.removeIndex,
		store.PublicBody.removeIndex
	], (err) => {
		if (err) return console.log(err);
		console.log('done.');
	});
});
