#!/usr/bin/env node

/*

 Checks Import Packages for valid format with the JSON Schema Definition File

*/

const lzma = require('lzma-native');
const fs = require('fs');
const async = require('async');
const path = require('path');
const config = require('../config.js');
const convert = require('../lib/convert.js');
const Ajv = require('ajv');
const ajv = new Ajv({verbose: true, jsonPointers: true, allErrors: true});

const data_path = config.data.tenderapi + '/import/';
const schema = JSON.parse(fs.readFileSync(config.data.shared + '/schema.json').toString());
const validate = ajv.compile(schema);
const stats = {
	count: 0,
	countries: {}
};

const check = (filename, cb) => {
	console.log('checking', filename);
	fs.readFile(data_path + filename, (err, content) => {
		lzma.decompress(content, (decompressedResult) => {
			let array = JSON.parse(decompressedResult.toString());
			stats.count = stats.count + array.length;
			array.forEach(tender => {
				stats.countries[tender.country] = (stats.countries[tender.country] || 0) + 1;
			});
			array = convert.cleanTenderApiDocs(array, stats);
			if (!validate(array)) {
				console.log(validate.errors);
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
		let filename = path.resolve('./check_data_results.json');
		fs.writeFileSync(filename, JSON.stringify(stats, null, '\t'));
		console.log('stats written', filename);
		console.log('done.')
	});
});

