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
const Utils = require('../lib/utils');
const CSV = require('../lib/csv');
const Library = require('../lib/library.js');

const now = (new Date()).valueOf();

const library = new Library(config);
const csv = new CSV(library);


let currentCountry = '-';
let status_items = status.addItem('items');
status.addItem('country', {
	custom: () => {
		return currentCountry;
	}
});
let status_portals = status.addItem('portals');

let downloadsFolder = path.join(config.data.shared, 'downloads');

let portals = JSON.parse(fs.readFileSync(path.join(config.data.shared, 'portals.json')).toString()).reverse();

let store = new Store(config);

let results = [];

let compressStream = (stream) => {
	let compress = zlib.createGzip();
	compress.pipe(stream);
	return compress;
};

let streamItems = (country_id, onItems, onEnd) => {
	let query = {match_all: {}};
	if (country_id.toUpperCase() !== 'ALL') {
		query = {term: {'ot.country': country_id.toUpperCase()}};
	}
	let pos = 0;
	store.Tender.streamQuery(1000, query,
		(items, total) => {
			pos += items.length;
			if (!onItems(items, pos, total)) {
				return false;
			}
			status_items.count = pos;
			status_items.max = total;
			return true;
		},
		(err) => {
			onEnd(err, pos);
		});
};

let downloadFolderFileStream = (filename) => {
	let fullFilename = path.join(downloadsFolder, filename);
	let outputStream = fs.createWriteStream(fullFilename);
	return outputStream;
};

let dump = (country, cb) => {
	currentCountry = country.name;
	let countryId = (country.id ? country.id.toLowerCase() : 'all');
	let filename = 'data-' + countryId;
	console.log('Saving downloads for ' + currentCountry);

	let file_ndjson = {filename: filename + '.ndjson.gz', size: 0};
	let file_ndjson_stream = downloadFolderFileStream(file_ndjson.filename);
	let file_ndjson_compress = compressStream(file_ndjson_stream);
	let file_ndjson_serialize = ndjson.serialize();
	file_ndjson_serialize.on('data', line => {
		file_ndjson_compress.write(line);
	});

	let file_json = {filename: filename + '.json.gz', size: 0};
	let file_json_stream = downloadFolderFileStream(file_json.filename);
	let file_json_compress = compressStream(file_json_stream);
	file_json_compress.write('[');

	let file_csv = {filename: filename + '.csv.gz', size: 0};
	let file_csv_stream = downloadFolderFileStream(file_csv.filename);
	let file_csv_compress = compressStream(file_csv_stream);
	file_csv_compress.write(csv.header());

	let totalItems = 0;

	file_ndjson_stream.on('close', () => {
		setTimeout(() => {
			file_json.size = fs.statSync(path.join(downloadsFolder, file_json.filename)).size;
			file_ndjson.size = fs.statSync(path.join(downloadsFolder, file_ndjson.filename)).size;
			file_csv.size = fs.statSync(path.join(downloadsFolder, file_csv.filename)).size;
			let result = {country: countryId, count: totalItems, lastUpdate: now, formats: {json: file_json, ndjson: file_ndjson, csv: file_csv}};
			results.push(result);
			cb();
		}, 1000);
	});

	streamItems(countryId,
		(items, pos, total) => {
			if (totalItems !== 0) {
				file_json_compress.write(',');
			}
			items.forEach((item, index) => {
				Utils.cleanOtFields(item._source);
				file_ndjson_serialize.write(item._source);
				file_csv_compress.write(csv.transform(item._source, pos + index));
			});
			file_json_compress.write(items.map(item => {
				return JSON.stringify(item._source);
			}).join(','));
			status_items.count = pos;
			status_items.max = total;
			totalItems = total;
			return true;
		}, (err, total) => {
			totalItems = total;
			if (err) {
				console.log('error streaming tenders', err);
			} else {
				file_json_compress.write(']');
			}
			file_csv_compress.end();
			file_json_compress.end();
			file_ndjson_serialize.end();
			file_ndjson_compress.end();
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
