const status = require('node-status');

let status_items = status.addItem('items', {type: ['count']});

const Importer = function (store, index, silent, showProgress) {
	let me = this;

	me.setTotal = function (n) {
		status_items.max = n;
	};

	me.setCount = function (n) {
		status_items.count = n;
	};

	me.add = function (doc, cb) {
		index.add(doc, function (err) {
			if (err) {
				return cb(err);
			}
			if (!silent) status_items.inc();
			cb();
		});
	};

	me.bulk = function (docs, cb) {
		index.bulk_add(docs, function (err) {
			if (err) {
				return cb(err);
			}
			if (!silent) status_items.inc(docs.length);
			cb();
		});
	};

	me.open = function (cb) {
		if (!silent && showProgress) status.start();
		store.init((err) => {
			if (err) return cb(err);
			index.removeIndex((err) => {
				if (err) return cb(err);
				index.checkIndex(cb);
			});
		});
	};

	me.close = function (cb) {
		if (!silent && showProgress) status.stop();
		cb();
	};

	me.start = function (process) {
		if (!silent && showProgress) status.start();
		store.init((err) => {
			if (err) {
				if (!silent && showProgress) status.stop();
				return process(err);
			}
			index.removeIndex((err) => {
				if (err) return process(err);
				index.checkIndex((err) => {
					if (err) return process(err);
					process(null, (err) => {
						if (err) return console.log(err);
						store.close(() => {
							if (!silent && showProgress) status.stop();
							console.log('end.');
						});
					});
				});
			});
		});
	};

	me.execute = function (process, cb) {
		if (!silent && showProgress) status.start();
		store.init((err) => {
			if (err) {
				if (!silent && showProgress) status.stop();
				return console.log(err);
			}
			index.removeIndex((err) => {
				if (err) return console.log(err);
				index.checkIndex((err) => {
					if (err) return console.log(err);
					process((err) => {
						if (err) return console.log(err);
						store.close(() => {
							if (!silent && showProgress) status.stop();
							console.log('end.');
						});
					});
				});
			});
		});
	};

	me.stop = function () {
		if (!silent && showProgress) status.stop();
	};

};

module.exports = Importer;