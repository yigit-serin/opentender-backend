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
const lzma = require('lzma-native');
const Ajv = require('ajv');
const status = require('node-status');
// const yaml = require('js-yaml');

const console = status.console();

const Store = require('../lib/store.js');
const Importer = require('../lib/importer.js');
const Library = require('../lib/library.js');
const Converter = require('../lib/convert.js');
const config = require('../config.js');
const package_length = 20000;

const validator = (filename) => {
	const ajv = new Ajv({verbose: true, jsonPointers: true, allErrors: true});
	const schema = JSON.parse(fs.readFileSync(path.resolve(config.data.shared, filename)).toString());
	return ajv.compile(schema);
};

const validateOpentender = validator('schema.json');
const validateTenderAPI = validator('tenderapi.json');

const data_path = config.data.tenderapi;
const showProgress = true;

const store = new Store(config);
const library = new Library(config);
const converter = new Converter(null, library, config.data.path);
const importerTender = new Importer(store, store.Tender, false, showProgress);
let total = 0;
let count = 0;
let stats = {};

let importTenderPackage = (array, filename, cb) => {
	// validate tenderapi
	let valid = validateTenderAPI(array);
	if (!valid) {
		console.log('tenderapi schema error in filename', filename);
		console.log(validateTenderAPI.errors);
	}

	// remove unused variables & clean some data
	array = converter.transform(array);

	// validate opentender
	valid = validateOpentender(array);
	if (!valid) {
		console.log('opentender schema error in filename', filename);
		console.log(validateOpentender.errors);
	}

	// update status ui
	if (array.length < package_length) {
		importerTender.setTotal(total - (package_length - array.length));
	}
	count += array.length;
	importerTender.setCount(count);

	array.forEach(item => {
		stats[item.country] = (stats[item.country] || 0) + 1;
	});

	let bulk_packages = [];
	while (array.length > 0) {
		let bulk_package = array.slice(0, 1000);
		bulk_packages.push(bulk_package);
		array = array.slice(1000);
	}
	async.forEachSeries(bulk_packages, (bulk_package, next) => {
		importerTender.bulk(bulk_package, (err) => {
			next(err);
		});
	}, (err) => {
		if (err) {
			console.error(err);
			return cb(err);
		}
		cb();
	});
};

let importTenderPackageFile = (filename, cb) => {
	// read & decompress a package file
	let fullfilename = path.join(data_path, 'import', filename);
	fs.readFile(fullfilename, (err, content) => {
		if (err) {
			console.error(err);
			return cb(err);
		}
		console.log('importing', fullfilename);
		lzma.decompress(content, decompressedResult => {
			let array = JSON.parse(decompressedResult.toString());
			importTenderPackage(array, filename, cb);
		});
	});
};

let importTenderPackageFiles = (cb) => {
	let nextPackageFilename = path.join(data_path, 'package_next.json');
	let package_next;
	// read package next json & all declared package files from it
	if (!fs.existsSync(nextPackageFilename)) {
		console.log('No import data file found', nextPackageFilename);
		return cb();
	}
	package_next = JSON.parse(fs.readFileSync(nextPackageFilename).toString());
	let importpackagefilename = path.join(data_path, 'package_' + package_next.timestamp.replace(/[\/:\.]/g, '-') + '.json');
	if (!fs.existsSync(importpackagefilename)) {
		return cb('nothing to import: ' + importpackagefilename + ' does not exists');
	}
	console.log('Processing package', importpackagefilename);
	let package_import = JSON.parse(fs.readFileSync(importpackagefilename).toString());
	let unique = [];
	let hasError = false;
	package_import.files.forEach(filename => {
		if (unique.indexOf(filename) < 0) {
			unique.push(filename);
		} else {
			console.error('invalid package, file', filename, 'is duplicated');
			hasError = true;
		}
		let fullfilename = path.join(data_path, 'import', filename);
		if (!fs.existsSync(fullfilename)) {
			console.error('invalid package, file', fullfilename, 'does not exists');
			hasError = true;
		}
	});
	if (hasError) {
		return;
	}
	total = package_length * package_import.files.length;
	importerTender.setTotal(total);
	async.forEachSeries(package_import.files, importTenderPackageFile, (err) => {
		if (count > 0) {
			console.log('Tender Country Stats:', JSON.stringify(stats));
		} else {
			console.error('Could not read any tenders');
		}
		cb();
	});
};

let openDB = (cb) => {
	async.waterfall([
		(next) => importerTender.open(next)
	], (err) => {
		cb(err);
	});
};

let closeDB = () => {
	importerTender.stop();
	async.waterfall([
		(next) => importerTender.close(next),
		(next) => store.close(next)
	], (err) => {
		console.log(err ? err : 'done.');
	});
};

openDB(err => {
	if (err) {
		console.log(err);
		return closeDB();
	}
	importTenderPackageFiles(err => {
		if (err) {
			console.log(err);
			return closeDB();
		}
		closeDB();
		console.log('done.');
	});
});
