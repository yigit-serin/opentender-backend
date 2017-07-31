const status = require('node-status');

let status_items = status.addItem('items', {type: ['count']});

class Importer {
	constructor(store, index, silent, showProgress) {
		this.store = store;
		this.index = index;
		this.silent = silent;
		this.showProgress = showProgress;
	}

	setTotal(n) {
		status_items.max = n;
	}

	setCount(n) {
		status_items.count = n;
	}

	add(doc, cb) {
		this.index.add(doc, (err) => {
			if (err) {
				return cb(err);
			}
			if (!this.silent) {
				status_items.inc();
			}
			cb();
		});
	}

	bulk(docs, cb) {
		this.index.bulk_add(docs, (err) => {
			if (err) {
				return cb(err);
			}
			if (!this.silent) {
				status_items.inc(docs.length);
			}
			cb();
		});
	}

	open(cb) {
		if (!this.silent && this.showProgress) {
			status.start();
		}
		this.store.init((err) => {
			if (err) {
				return cb(err);
			}
			this.index.removeIndex((err) => {
				if (err) {
					return cb(err);
				}
				this.index.checkIndex(cb);
			});
		});
	}

	close(cb) {
		if (!this.silent && this.showProgress) {
			status.stop();
		}
		cb();
	}

	start(process) {
		if (!this.silent && this.showProgress) {
			status.start();
		}
		this.store.init((err) => {
			if (err) {
				if (!this.silent && this.showProgress) {
					status.stop();
				}
				return process(err);
			}
			this.index.removeIndex((err) => {
				if (err) {
					return process(err);
				}
				this.index.checkIndex((err) => {
					if (err) {
						return process(err);
					}
					process(null, (err) => {
						if (err) {
							return console.log(err);
						}
						this.store.close(() => {
							if (!this.silent && this.showProgress) {
								status.stop();
							}
							console.log('end.');
						});
					});
				});
			});
		});
	};

	execute(process, cb) {
		if (!this.silent && this.showProgress) {
			status.start();
		}
		this.store.init((err) => {
			if (err) {
				if (!this.silent && this.showProgress) {
					status.stop();
				}
				return console.log(err);
			}
			this.index.removeIndex((err) => {
				if (err) {
					return console.log(err);
				}
				this.index.checkIndex((err) => {
					if (err) {
						return console.log(err);
					}
					process((err) => {
						if (err) {
							return console.log(err);
						}
						this.store.close(() => {
							if (!this.silent && this.showProgress) {
								status.stop();
							}
							console.log('end.');
						});
					});
				});
			});
		});
	};

	stop() {
		if (!this.silent && this.showProgress) {
			status.stop();
		}
	}
}

module.exports = Importer;
