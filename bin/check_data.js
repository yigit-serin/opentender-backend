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
const ajv = new Ajv({verbose: true, jsonPointers: true, allErrors: true});
const schema = JSON.parse(fs.readFileSync(config.data.shared + '/schema.json').toString());
const validate = ajv.compile(schema);
const stats = {
	count: 0,
	countries: {}
};
const library = new Library(config);
const converter = new Converter(stats, library);

const check = (filename, cb) => {
	console.log('checking', filename);
	fs.readFile(data_path + filename, (err, content) => {
		lzma.decompress(content, (decompressedResult) => {
			let array = JSON.parse(decompressedResult.toString());
			stats.count = stats.count + array.length;
			array.forEach(tender => {
				stats.countries[tender.country] = (stats.countries[tender.country] || 0) + 1;
			});
			array = converter.transform(array);
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

