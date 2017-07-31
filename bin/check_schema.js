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

const check = (filename, cb) => {
	console.log('checking', filename);
	fs.readFile(data_path + filename, (err, content) => {
		lzma.decompress(content, (decompressedResult) => {
			let array = JSON.parse(decompressedResult.toString());
			convert.cleanTenderApiDocs(array);
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
	async.forEachSeries(items, check, () => console.log('done.'));
});

