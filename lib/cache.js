class MemcachedAdapter {
	constructor(hosts) {
		const Memcached = require('memcached');
		this.memcached = new Memcached(hosts);
	}

	get(key, cb) {
		this.memcached.get(key, (err, result) => {
			cb(err, result ? result.data : null);
		});
	}

	upsert(key, data, duration, cb) {
		this.memcached.get(key, (err, result) => {
			if (err) {
				return cb(err);
			}
			if (!result) {
				this.memcached.set(key, {data: data}, duration || 0, (err) => {
					if (err) {
						return cb(err);
					}
					cb(null, true);
				});
			} else {
				cb(null, false);
			}
		});
	}
}

class NodeCacheAdapter {

	constructor() {
		this.nodecache = require('memory-cache');
		this.maximum_waittime = 2147483647; // no longer max wait time supported
	}

	get(key, cb) {
		let c = this.nodecache.get(key);
		cb(null, c ? c.data : null);
	}

	upsert(key, data, duration, cb) {
		let c = this.nodecache.get(key);
		if (!c) {
			duration = duration || 0;
			if (duration <= 0) {
				duration = this.maximum_waittime;
			}
			this.nodecache.put(key, {data: data}, Math.min(duration, this.maximum_waittime));
			cb(null, true);
		} else {
			cb(null, false);
		}
	}
}

class NullCacheAdapter {
	constructor() {
	}

	get(key, cb) {
		cb();
	}

	upsert(key, data, duration, cb) {
		cb(null, false);
	}
}

module.exports.initCache = (options) => {
	if (options.type === 'memcached') {
		return new MemcachedAdapter(options.memcached);
	}
	if (options.type === 'internal') {
		return new NodeCacheAdapter();
	}
	return new NullCacheAdapter();
};
