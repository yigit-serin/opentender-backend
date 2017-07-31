#!/usr/bin/env node

/*

 Saves the DB to country specific downloads packages

 */

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const async = require('async');
const ndjson = require('ndjson');
const status = require('node-status');
const Store = require('../lib/store.js');
const config = require('../config.js');
const console = status.console();

let currentCountry = '-';
let status_items = status.addItem('items');
status.addItem('country', {
	custom: () => {
		return currentCountry;
	}
});
let status_portals = status.addItem('portals');

let downloadsFolder = path.join(config.data.shared, 'downloads');

let portals = JSON.parse(fs.readFileSync(path.join(config.data.shared, 'portals.json')).toString()).active;

let store = new Store(config);

let results = [];

let dump = (country, cb) => {
	currentCountry = country.name;
	let countryId = (country.id ? country.id.toLowerCase() : 'eu');
	let filename = 'data-' + countryId + '.ndjson.gz';
	let fullFilename = path.join(downloadsFolder, filename);
	let outputStream = fs.createWriteStream(fullFilename);
	let compress = zlib.createGzip();
	let totalItems = 0;
	compress.pipe(outputStream);
	let serialize = ndjson.serialize();
	serialize.on('data', line => {
		compress.write(line);
	});
	outputStream.on('close', () => {
		setTimeout(() => {
			let result = {filename: filename, size: fs.statSync(fullFilename).size, country: countryId, count: totalItems};
			console.log(JSON.stringify(result));
			results.push(result);
			cb();
		}, 1000);
	});
	let query = {match_all: {}};
	if (country.id.toUpperCase() !== 'EU') {
		query = {term: {country: country.id.toUpperCase()}};
	}
	store.Tender.streamQuery(query,
		(item, pos, total) => {
			serialize.write(item._source);
			status_items.count = pos;
			status_items.max = total;
			totalItems = total;
		},
		(err) => {
			if (err) {
				console.log('error streaming tenders', err);
			}
			serialize.end();
			compress.end();
		});
};

store.init((err) => {
	if (err) {
		return console.log(err);
	}
	status.start(
		{pattern: 'Dumping: {uptime} | {spinner.cyan} | {items} | {country.custom} | {portals}'}
	);
	let pos = 0;
	status_portals.count = 0;
	status_portals.max = portals.length;
	async.forEachSeries(portals, (portal, next) => {
		status_portals.count = ++pos;
		// if (!portal.id) return next();
		dump(portal, next);
	}, err => {
		if (err) {
			console.log(err);
		}
		store.close(() => {
			status.stop();
			fs.writeFileSync(path.join(downloadsFolder, 'downloads.json'), JSON.stringify(results, null, '\t'));
			console.log('done');
		});
	});
});
