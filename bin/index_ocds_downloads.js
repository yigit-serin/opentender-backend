const fs = require('fs');
const async = require('async');
const path = require('path');
const config = require('../config.js');
const download_path = config.data.shared + '/downloads/';

const downloads = [];

fs.readdir(download_path, (err, items) => {
	if (err) {
		return console.error(err);
	}
	items = items.filter((item) => {
		return (item.indexOf('ocds_data.json.tar.gz') > 0);
	});
	async.forEachSeries(items, (item, next) => {
		let stats = fs.statSync(path.join(download_path, item));
		let info = {
			filename: item,
			// count: 0,
			lastUpdate: stats.mtimeMs,
			size: stats.size
		};
		downloads.push(info);
		next();
	}, () => {
		let filename = path.resolve(download_path, 'downloads_ocds.json');
		fs.writeFileSync(filename, JSON.stringify(downloads, null, '\t'));
		console.log('downloads_ocds written', filename);
		console.log('done.')
	});
});

