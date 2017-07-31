const fs = require('fs');
const path = require('path');
const async = require('async');
const config = require('../config.js');

// git clone https://github.com/Alir3z4/stop-words ../data/backend/stop-words

const langs = ['bulgarian', 'czech', 'danish', 'dutch', 'english', 'finnish', 'french', 'german', 'hungarian', 'italian',
	'norwegian', 'polish', 'portuguese', 'romanian', 'slovak', 'spanish', 'swedish'];

let all = [];
async.forEachSeries(langs, (lang, next) => {
	let filename = path.join(config.data.path, 'stop-words', lang + '.txt');
	console.log(filename);
	let li = fs.readFileSync(filename).toString().split('\n');
	li.forEach(text => {
		if (text.length > 2 && all.indexOf(text) < 0) {
			all.push(text);
		}
	});
	next();
}, () => {
	if (all.length > 0) {
		fs.writeFileSync(path.join(config.data.path, 'stopwords.txt'), all.join('\n'));
		console.log('done');
	} else {
		console.log('nothing to write found');
	}
});
