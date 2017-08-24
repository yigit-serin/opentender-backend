#!/usr/bin/env node

const fs = require('fs');
const express = require('express');
const path = require('path');
const cache = require('memory-cache');
const bodyParser = require('body-parser');
const helmet = require('helmet');

const Api = require('./lib/api.js');
const config = require('./config.js');

let portals = JSON.parse(fs.readFileSync(path.join(config.data.shared, 'portals.json')).toString());
let useCache = !config.disableCache;

let api = new Api(config);
let app = express();

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

let addToCache = (req, data) => {
	if (!useCache) {
		return;
	}
	let url = req.url + '|' + JSON.stringify(req.body) + JSON.stringify(req.params);
	// console.log('add to cache', url);
	let c = cache.get(url);
	if (!c) {
		let maximum_waittime = 2147483647; // TODO: switch to external memcached
		cache.put(url, {url: url, data: data}, maximum_waittime);
	}
};

let sendAndAddToCache = (req, res, data) => {
	addToCache(req, data);
	res.send(data);
};

let checkCache = (req, res, cb) => {
	if (!useCache) {
		return cb();
	}
	let url = req.url + '|' + JSON.stringify(req.body) + JSON.stringify(req.params);
	let c = cache.get(url);
	if (c) {
		// console.log('request found in cache', url);
		res.send(c.data);
	} else {
		// console.log('request NOT found in cache', url);
		cb();
	}
};

app.get('/', (req, res) => {
	res.send('OpenTender Data Server');
});

app.get('/api/test.json', (req, res) => {
	res.send({ok: 'yes'});
});

let processAnswer = (req, res, err, data) => {
	if (err) {
		console.log(err);
		if (err === 404) {
			return res.sendStatus(404);
		}
		return res.sendStatus(500);
	}
	sendAndAddToCache(req, res, {data: data});
};

app.get('/api/portals/list', (req, res) => {
	processAnswer(req, res, null, portals);
});

let processPortalsStats = (data) => {
	if (!data) {
		return null;
	}
	let count_all = 0;
	portals.forEach((p) => {
		if (p.id !== 'eu') {
			count_all += data[p.id] || 0;
		}
	});
	data['eu'] = count_all;
	let list = portals.map(p => {
		return {
			id: p.id,
			name: p.name,
			value: data[p.id] || 0
		};
	});
	list.sort((a, b) => {
		if (a.id === 'eu') {
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
	api.getCountriesStats((err, data) => {
		processAnswer(req, res, err, processPortalsStats(data));
	});
});

let registerCountryApi = country => {
	let api_path = '/api/' + (country.id || 'eu') + '/';
	let country_id = (country.id && (country.id !== 'eu') ? country.id.toUpperCase() : null);

	app.post(api_path + 'tender/search', checkCache, (req, res) => {
		api.searchTender(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.post(api_path + 'company/search', checkCache, (req, res) => {
		api.searchCompany(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'company/nuts', checkCache, (req, res) => {
		api.getCompanyNutsStats(country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.post(api_path + 'authority/search', checkCache, (req, res) => {
		api.searchAuthority(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'authority/nuts', checkCache, (req, res) => {
		api.getAuthorityNutsStats(country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'sector/list/main', checkCache, (req, res) => {
		api.getCPVUsageStats(country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.post(api_path + 'autocomplete', checkCache, (req, res) => {
		api.autocomplete(req.body.entity, req.body.field, req.body.search, country_id, (err, data) => {
			processAnswer(req, res, err, data);
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

	app.post(api_path + 'region/stats', checkCache, (req, res) => {
		api.getRegionStats(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'tender/id/:id', checkCache, (req, res) => {
		api.getTender({id: req.params.id, lang: req.query.lang}, (err, data) => {
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

	app.post(api_path + 'company/nuts', checkCache, (req, res) => {
		api.getCompanyNutsStats(country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.post(api_path + 'indicators/stats', checkCache, (req, res) => {
		api.getIndicatorStats(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'company/similar/:id', checkCache, (req, res) => {
		api.searchSimilarCompany(req.params.id, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'authority/id/:id', checkCache, (req, res) => {
		api.getAuthority(req.params.id, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.post(api_path + 'authority/stats', checkCache, (req, res) => {
		api.getAuthorityStats(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'authority/id/:id', checkCache, (req, res) => {
		api.searchSimilarAuthority(req.params.id, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'quality/usage', checkCache, (req, res) => {
		api.getFieldsUsage(country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'location/map.geo.json', checkCache, (req, res) => {
		api.getLocationsMap((err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'company/similar/:id', checkCache, (req, res) => {
		api.searchSimilarCompany(req.params.id, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'authority/similar/:id', checkCache, (req, res) => {
		api.searchSimilarAuthority(req.params.id, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

};
portals.forEach(registerCountryApi);

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
		console.log('Opentender Api is listening on: http://%s:%d (%s)', listener.address().address, listener.address().port, app.settings.env);
		api.getCountriesStats((err, countries) => {
			if (countries) {
				let unused = {};
				Object.keys(countries).forEach(key => {
					let portal = portals.find(p => p.id === key);
					if (!portal) {
						unused[key] = countries[key];
					}
				});
				if (Object.keys(unused).length > 0) {
					console.log('Unused Portal Data available with not supported/invalid country codes:');
					console.log(JSON.stringify(unused));
				}
			}
		});
	});
});
