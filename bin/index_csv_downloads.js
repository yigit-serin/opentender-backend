const fs = require('fs');
const async = require('async');
const path = require('path');
const config = require('../config.js');
const download_path = config.data.shared + '/downloads/';
const csv = require('csv-streamify');

const downloads = [];

let indexCSV = (filename, cb) => {
	console.log('reading', download_path + filename);

	//files are too big for nodejs string, so csv must be streamed to count nr of lines
	const parser = csv();
	let info = {
		filename: filename,
		count: 0,
		size: 0
	};
	parser.on('data', function (line) {
		info.count++;
	});
	parser.on('end', function () {
		let stats = fs.statSync(path.join(download_path, filename));
		info.size = stats.size;
		info.lastUpdate = stats.mtimeMs;
		downloads.push(info);
		console.log(info);
		cb();
	});

	fs.createReadStream(download_path + filename).pipe(parser);
};

fs.readdir(download_path, (err, items) => {
	if (err) {
		return console.error(err);
	}
	items = items.filter((item) => {
		return (path.extname(item) === '.csv');
	});
	async.forEachSeries(items, indexCSV, () => {
		let filename = path.resolve(download_path,'downloads_csv.json');
		fs.writeFileSync(filename, JSON.stringify(downloads, null, '\t'));
		console.log('downloads written', filename);
		console.log('done.')
	});
});

