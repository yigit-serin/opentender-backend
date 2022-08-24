#!/usr/bin/env node

const fs = require('fs');
const express = require('express');
const path = require('path');
const async = require('async');
const bodyParser = require('body-parser');
const helmet = require('helmet');
const morgan = require('morgan');

const Api = require('./lib/api.js');
const Cache = require('./lib/cache.js');
const config = require('./config.js');
const crypto = require('crypto');
const pck = require('./package.json');

let portals = JSON.parse(fs.readFileSync(path.join(config.data.shared, 'portals.json')).toString());
let portals_geojson = JSON.parse(fs.readFileSync(path.join(config.data.shared, 'countries.geo.json')).toString());

portals_geojson.features = portals_geojson.features.filter(feature => {
	let id = (feature.properties.iso_a2 || '').toLowerCase();
	let p = portals.find(portal => {
		return portal.id === id;
	});
	return p;
}).map(feature => {
	return {
		type: feature.type,
		geometry: feature.geometry,
		properties: {id: feature.properties.iso_a2.toLowerCase()}
	}
});

let api = new Api(config);
let app = express();

let cache = Cache.initCache(config.cache);

app.use(helmet());
app.all('*', (req, res, next) => {
	res.header('Access-Control-Allow-Origin', '*');
	res.header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
	res.header('Access-Control-Allow-Headers', 'Content-Type');
	if (req.method === 'OPTIONS') {
		res.sendStatus(200);
	} else {
		next();
	}
});
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: false}));

if (app.settings.env !== 'production') {

	morgan.token('cached', (req) => {
		return req.cached ? 'true' : 'false';
	});

	app.use(morgan('[:date[clf]] - cached: :cached - :method :url - :res[content-length] - :response-time ms'));
}

let md5hash = (value) => {
	return crypto.createHash('md5').update(value).digest('hex');
};

let getCacheKey = (req) => {
	let url = req.originalUrl.split('?')[0];
	let query = {lang: req.query.lang};
	let key = JSON.stringify({u: url, q: query, b: req.body, p: req.params});
	return md5hash(key);
};

let addToCache = (req, data, duration) => {
	cache.upsert(getCacheKey(req), data, duration, (err, stored) => {
		if (err) {
			if (err.toString().indexOf('The length of the value is greater than') > 0) {
				return console.error('Could not cache', req.originalUrl, ' - too large:', JSON.stringify(data).length);
			}
			return console.error(err, req.originalUrl);
		}
	});
};

let sendAndAddToCache = (req, res, data, duration) => {
	addToCache(req, data, duration);
	res.send(data);
};

let checkCache = (req, res, cb) => {
	cache.get(getCacheKey(req), (err, result) => {
		if (err) {
			console.error(err);
			return cb();
		}
		if (!result) {
			cb();
		} else {
			req.cached = true;
			res.send(result);
		}
	});
};

app.get('/', (req, res) => {
	res.send('OpenTender Data Server ' + pck.version);
});

app.get('/api/test.json', (req, res) => {
	res.send({version: pck.version, ok: 'yes'});
});

let short_cache_duration = 5 * 60 * 1000;

let processAnswer = (req, res, err, data, duration) => {
	if (err) {
		console.log(err, req.originalUrl);
		if (err === 404) {
			return res.sendStatus(404);
		}
		return res.sendStatus(500);
	}
	sendAndAddToCache(req, res, {data: data}, duration);
};

app.get('/api/portals/geo.json', checkCache, (req, res) => {
	sendAndAddToCache(req, res, portals_geojson);
});

let processPortalsStats = (data, lang) => {
	if (!data) {
		return null;
	}
	let count_all = 0;
	portals.forEach((p) => {
		if (p.id !== 'all') {
			count_all += data[p.id] || 0;
		}
	});
	data['all'] = count_all;
	let list = portals.map(p => {
		return {
			id: p.id,
			name: p.names[lang] || p.name,
			value: data[p.id] || 0
		};
	});
	list.sort((a, b) => {
		if (a.id === 'all') {
			return -1;
		}
		if (a.name < b.name) {
			return -1;
		}
		if (a.name > b.name) {
			return 1;
		}
		return 0;
	});
	return list;
};

app.get('/api/portals/stats', checkCache, (req, res) => {
	api.getCountriesStats((err, stats) => {
		let data = processPortalsStats(stats, req.query ? req.query.lang : 'en');
		processAnswer(req, res, err, data);
	});
});

app.get('/api/portals/usage', checkCache, (req, res) => {
	api.getCountriesStats((err, countries) => {
		if (countries) {
			let unused = {};
			let used = {};
			Object.keys(countries).forEach(key => {
				let portal = portals.find(p => p.id === key);
				if (!portal) {
					unused[key] = countries[key];
				} else {
					used[key] = countries[key];
				}
			});
			let data = {used, unused};
			processAnswer(req, res, err, data);
		}
	});
});


let downloads = {};
let download_queue = async.queue((task, next) => {
	if (task.request) {
		const id = md5hash(JSON.stringify(task.req.body) + (new Date()).valueOf());
		downloads[id] = {body: task.req.body, format: task.format};
		setTimeout(() => {
			// expires after 60 seconds
			delete downloads[id];
		}, 60000);
		task.res.send({data: {id: id}});
		next();
	} else {
		if (task.format === 'csv') {
			api.streamTenderCSV(task.id, task.req, task.res, task.body, task.country_id, next);
		} else {
			api.streamTenderJSON(task.id, task.req, task.res, task.body, task.country_id, next);
		}
	}
}, 6);

let registerCountryApi = country_id => {
	let api_path = '/api/';

	app.post(api_path + 'autocomplete', checkCache, (req, res) => {
		api.autocomplete(req.body.entity, req.body.field, req.body.search, country_id, (err, data) => {
			processAnswer(req, res, err, data, short_cache_duration);
		});
	});

	app.post(api_path + 'market/stats', checkCache, (req, res) => {
		api.getMarketAnalysisStats(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.post(api_path + 'home/stats', checkCache, (req, res) => {
		api.getHomeStats(country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.post(api_path + 'region/stats', checkCache, (req, res) => {
		api.getRegionStats(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});


	app.post(api_path + 'indicators/range-stats', checkCache, (req, res) => {
		api.getIndicatorRangeStats(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.post(api_path + 'indicators/score-stats', checkCache, (req, res) => {
		api.getIndicatorScoreStats(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});


	app.get(api_path + 'sector/id/:id', checkCache, (req, res) => {
		api.getCPV({id: req.params.id, lang: req.query.lang}, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.post(api_path + 'sector/stats', checkCache, (req, res) => {
		api.getSectorStats(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'sector/list/main', checkCache, (req, res) => {
		api.getCPVUsageStats(country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});


	app.get(api_path + 'tender/id/:id', checkCache, (req, res) => {
		api.getTender({id: req.params.id, lang: req.query.lang}, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.post(api_path + 'tender/stats', checkCache, (req, res) => {
		api.getTenderStats(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.post(api_path + 'tender/search', checkCache, (req, res) => {
		api.searchTender(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data, short_cache_duration);
		});
	});

	app.post(api_path + 'tender/download/json', (req, res) => {
		download_queue.push({request: true, format: 'json', req, res, country_id});
	});

	app.post(api_path + 'tender/download/csv', (req, res) => {
		download_queue.push({request: true, format: 'csv', req, res, country_id});
	});

	app.get(api_path + 'download/id/:id', (req, res) => {
		let download = downloads[req.params.id];
		if (!download) {
			return res.status(401).send('download token invalid/expired');
		}
		delete downloads[req.params.id];
		download_queue.push({id: req.params.id, format: download.format, req, res, body: download.body, country_id});
	});


	app.get(api_path + 'authority/id/:id', checkCache, (req, res) => {
		api.getAuthority(req.params.id, country_id, (err, data) => {
			processAnswer(req, res, err, data, short_cache_duration);
		});
	});

	app.post(api_path + 'authority/stats', checkCache, (req, res) => {
		api.getAuthorityStats(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'authority/similar/:id', checkCache, (req, res) => {
		api.searchSimilarAuthority(req.params.id, country_id, (err, data) => {
			processAnswer(req, res, err, data, short_cache_duration);
		});
	});

	app.post(api_path + 'authority/search', checkCache, (req, res) => {
		api.searchBuyer(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'authority/nuts', checkCache, (req, res) => {
		api.getAuthorityNutsStats(country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});


	app.get(api_path + 'company/id/:id', checkCache, (req, res) => {
		api.getCompany(req.params.id, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.post(api_path + 'company/stats', checkCache, (req, res) => {
		api.getCompanyStats(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'company/nuts', checkCache, (req, res) => {
		api.getCompanyNutsStats(country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'company/similar/:id', checkCache, (req, res) => {
		api.searchSimilarCompany(req.params.id, country_id, (err, data) => {
			processAnswer(req, res, err, data, short_cache_duration);
		});
	});

	app.post(api_path + 'company/search', checkCache, (req, res) => {
		api.searchSupplier(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});
};
registerCountryApi(config.country.code);

// error handlers

app.use((req, res) => {
	res.status(404);
	if (req.accepts('json')) {
		res.send({error: 'Not found'});
		return;
	}
	res.type('txt').send('Not found');
});

app.use((err, req, res) => {
	res.status(err.status || 500);
});

api.init(err => {
	if (err) {
		return console.log('Opentender Api Error', err);
	}
	const listener = app.listen(config.listen.port, config.listen.host, () => {
		console.log('Opentender Api ' + pck.version + ' is listening on: http://%s:%d (%s)', listener.address().address, listener.address().port, app.settings.env);
	});
});
