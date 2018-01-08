#!/usr/bin/env node

/*

 Checks Import Packages for valid format with the JSON Schema Definition File

*/

const lzma = require('lzma-native');
const Ajv = require('ajv');
const fs = require('fs');
const async = require('async');
const path = require('path');
const config = require('../config.js');
const Library = require('../lib/library.js');
const Converter = require('../lib/convert.js');

const data_path = config.data.tenderapi + '/import/';
const portals = JSON.parse(fs.readFileSync(path.resolve(config.data.shared, 'portals.json')).toString());
const portalIDs = portals.map(portal => portal.id.toUpperCase());

const validator = (filename) => {
	const ajv = new Ajv({verbose: true, jsonPointers: true, allErrors: true});
	const schema = JSON.parse(fs.readFileSync(path.resolve(config.data.shared, filename)).toString());
	return ajv.compile(schema);
};

const validateOpentender = validator('schema.json');
const validateTenderAPI = validator('tenderapi.json');

const stats = {
	count: 0,
	countries: {
		used: {},
		used_count: 0,
		unused: {},
		unused_count: 0
	}
};
const library = new Library(config);
const converter = new Converter(stats, library, config.data.path);

const check = (filename, cb) => {
	console.log('checking', filename);
	fs.readFile(data_path + filename, (err, content) => {
		lzma.decompress(content, (decompressedResult) => {
			let array = JSON.parse(decompressedResult.toString());
			stats.count = stats.count + array.length;
			if (!validateTenderAPI(array)) {
				console.log(validateTenderAPI.errors);
				return;
			}
			array = converter.transform(array);
			array.forEach(tender => {
				if (portalIDs.indexOf(tender.country) >= 0) {
					stats.countries.used[tender.country] = (stats.countries.used[tender.country] || 0) + 1;
					stats.countries.used_count++;
				} else {
					stats.countries.unused[tender.country] = (stats.countries.unused[tender.country] || 0) + 1;
					stats.countries.unused_count++;
				}
			});
			if (!validateOpentender(array)) {
				console.log(validateOpentender.errors);
				return;
			}
			cb();
		});
	});
};

fs.readdir(data_path, (err, items) => {
	items = items.filter((item) => {
		return (path.extname(item) === '.xz');
	});
	async.forEachSeries(items, check, () => {
		let filename = path.resolve('./check_data_results_' + (new Date()) + '.json');
		fs.writeFileSync(filename, JSON.stringify(stats, null, '\t'));
		console.log('stats written', filename);
		console.log('done.')
	});
});

