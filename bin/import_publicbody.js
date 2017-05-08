#!/usr/bin/env node

/*

 Import public body lists from FOI portals into the DB

 */

const fs = require('fs');
const path = require('path');
const async = require('async');

const Store = require('../lib/store.js');
const Importer = require('../lib/importer.js');
const config = require('../config.js');

let store = new Store(config);
let importer = new Importer(store, store.PublicBody);

const portals = require(config.data.shared + '/portals.json').active.filter(portal => portal.foi !== null);

let all = [];

let load = (portal, next) => {
	const filename = path.resolve(config.data.shared, 'publicbodies', portal.id.toLowerCase() + '_publicbodies.json');
	if (!fs.existsSync(filename)) {
		return next();
	}
	let objs = JSON.parse(fs.readFileSync(filename), toString());
	objs.forEach(o => {
		o.country = portal.id;
	});
	all = all.concat(objs);
	next();
};


let doIt = function (cb) {
	async.forEachSeries(portals, load, (err) => {
		if (err) {
			return cb(err);
		}
		importer.setTotal(all.length);
		importer.setCount(0);
		async.forEachSeries(all, importer.add, cb);
	});
};

importer.start(doIt);
