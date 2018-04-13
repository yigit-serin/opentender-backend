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
const archiver = require('archiver');

const now = (new Date()).valueOf();

const library = new Library(config);
const csv = new CSV(library);


let currentCountry = '-';
let currentAction = '';
let status_items = status.addItem('items');
status.addItem('country', {
	custom: () => {
		return currentCountry;
	}
});
let status_portals = status.addItem('portals');
status.addItem('current', {
	custom: () => {
		return currentAction;
	}
});

let downloadsFolder = path.join(config.data.shared, 'downloads');

let portals = JSON.parse(fs.readFileSync(path.join(config.data.shared, 'portals.json')).toString()).reverse();

let store = new Store(config);

let results = [];

let compressStream = (stream) => {
	let compress = zlib.createGzip();
	compress.pipe(stream);
	return compress;
};

let uncompressStream = (stream) => {
	let uncompress = zlib.createGunzip();
	stream.pipe(uncompress);
	return uncompress;
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


class DownloadPack {
	constructor(format, filename) {
		this.format = format;
		this.filename = filename;
		this.zipfilename = filename + '-' + this.format + '.zip';
		this.streams = {};
	}

	openStream(filename) {
		let filestream = downloadFolderFileStream(filename);
		let stream = compressStream(filestream);
		stream.orgstream = filestream;
		return stream;
	}

	closeStream(stream, cb) {
		stream.orgstream.on('close', (err) => {
			cb();
		});
		stream.end();
	}

	validateStream(year) {
		if (!this.streams[year]) {
			let filename = this.filename + '-' + year + '.' + this.format;
			this.streams[year] = {
				filename: filename,
				stream: this.openStream(filename + '.gz'),
				count: 0
			};
		}
		return this.streams[year];
	}

	write(year, data) {
		let yearstream = this.validateStream(year);
		yearstream.stream.write(data);
		yearstream.count++;
	}

	zip(cb) {
		let toZipFilenames = Object.keys(this.streams).map(key => this.streams[key].filename);
		const stream = downloadFolderFileStream(this.zipfilename);
		const archive = archiver('zip', {
			// store: true
			zlib: {level: 9} // Sets the compression level.
		});
		stream.on('close', function () {
			currentAction = '';
			// console.log(archive.pointer() + ' total bytes');
			cb();
		});
		stream.on('end', function () {
			// console.log('Data has been drained');
		});
		archive.on('entry', function (entry) {
			currentAction = 'Zipping: ' + entry.name;
		});
		archive.on('warning', function (err) {
			console.log(err);
		});
		archive.on('error', function (err) {
			console.log(err);
		});
		archive.on('progress', function (progress) {
			// console.log(progress);
		});
		toZipFilenames.forEach(toZipFilename => {
			let fullFilename = path.join(downloadsFolder, toZipFilename + '.gz');
			let filestream = fs.createReadStream(fullFilename);
			let stream = uncompressStream(filestream);
			archive.append(stream, {name: toZipFilename});
			// archive.file(fullFilename, {name: toZipFilename});
		});
		// console.log('Zipping', filename, 'Files:', JSON.stringify(toZipFilenames));
		archive.pipe(stream);
		archive.finalize();
	}

	removeFiles(cb) {
		async.forEach(Object.keys(this.streams), (key, next) => {
			let fullFilename = path.join(downloadsFolder, this.streams[key].filename + '.gz');
			fs.unlink(fullFilename, (err) => {
				next(err);
			});
		}, (err) => {
			cb(err)
		});
	}

	closeStreams(cb) {
		async.forEach(Object.keys(this.streams), (key, next) => {
			this.closeStream(this.streams[key].stream, next);
		}, (err) => {
			cb(err)
		});
	}

	close(cb) {
		this.closeStreams((err) => {
			if (err) {
				return cb(err);
			}
			this.zip((err) => {
				if (err) {
					return cb(err);
				}
				this.removeFiles((err) => {
					cb(err, {
						filename: this.zipfilename,
						size: fs.statSync(path.join(downloadsFolder, this.zipfilename)).size
					});
				});
			});
		});
	}

	writeTender(data, index, total) {

	}
}

class DownloadPackCSV extends DownloadPack {
	constructor(filename) {
		super('csv', filename);
	}

	writeTender(year, tender) {
		let yearstream = this.validateStream(year);
		this.write(year, csv.transform(tender, yearstream.count + 1));
	}

}

class DownloadPackJSON extends DownloadPack {

	constructor(filename) {
		super('json', filename);
	}

	openStream(filename) {
		let filestream = downloadFolderFileStream(filename);
		let stream = compressStream(filestream);
		stream.orgstream = filestream;
		stream.write('[');
		return stream;
	}

	closeStream(stream, cb) {
		stream.write(']');
		stream.orgstream.on('close', (err) => {
			cb();
		});
		stream.end();
	}

	writeTender(year, tender) {
		let yearstream = this.validateStream(year);
		this.write(year, (yearstream.count !== 0 ? ',' : '') + JSON.stringify(tender));
	}

}

class DownloadPackNDJSON extends DownloadPack {
	constructor(filename) {
		super('ndjson', filename);
	}

	openStream(filename) {
		let filestream = downloadFolderFileStream(filename);
		let compressstream = compressStream(filestream);
		let stream = ndjson.serialize();
		stream.compressstream = compressstream;
		stream.orgstream = filestream;
		stream.on('data', line => {
			compressstream.write(line);
		});
		return stream;
	}

	closeStream(stream, cb) {
		stream.orgstream.on('close', () => {
			cb();
		});
		stream.end();
		stream.compressstream.end();
	}

	writeTender(year, tender) {
		this.write(year, tender);
	}

}

let dump = (country, cb) => {
	currentCountry = country.name;
	let countryId = (country.id ? country.id.toLowerCase() : 'all');
	let filename = 'data-' + countryId;
	console.log('Saving downloads for ' + currentCountry);

	let totalItems = 0;
	let csvpack = new DownloadPackCSV(filename);
	let jsonpack = new DownloadPackJSON(filename);
	let ndjsonpack = new DownloadPackNDJSON(filename);

	currentAction = 'Streaming: ' + filename;
	streamItems(countryId,
		(items, pos, total) => {
			items.forEach((item, index) => {
				let year = (item._source.ot.date || '').slice(0, 4);
				if (year.length !== 4) {
					year = 'year-unavailable';
				}
				Utils.cleanOtFields(item._source);
				csvpack.writeTender(year, item._source);
				jsonpack.writeTender(year, item._source);
				ndjsonpack.writeTender(year, item._source);
			});
			status_items.count = pos;
			status_items.max = total;
			totalItems = total;
			return true;
		}, (err, total) => {
			totalItems = total;
			csvpack.close((err, file_csv) => {
				jsonpack.close((err, file_json) => {
					ndjsonpack.close((err, file_ndjson) => {
						let result = {
							country: countryId,
							count: totalItems,
							lastUpdate: now,
							formats: {
								json: file_json,
								ndjson: file_ndjson,
								csv: file_csv
							}
						};
						results.push(result);
						cb();
					});
				});
			});
		});
};

store.init((err) => {
	if (err) {
		return console.log(err);
	}
	status.start(
		{pattern: '@{uptime} | {spinner.cyan} | {items} | {country.custom} | {portals} | {current.custom}'}
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
			fs.writeFileSync(path.join(downloadsFolder, 'test-downloads.json'), JSON.stringify(results, null, '\t'));
			console.log('done');
		});
	});
});
