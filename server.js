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
	if ('OPTIONS' === req.method) {
		res.sendStatus(200);
	} else {
		next();
	}
});
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: false}));

let addToCache = (req, data) => {
	if (!useCache) return;
	let url = req.url + '?' + JSON.stringify(req.body) + JSON.stringify(req.params);
	// console.log('add to cache', url);
	let c = cache.get(url);
	if (!c) {
		let maximum_waittime = 2147483647;
		cache.put(url, {url: url, data: data}, maximum_waittime); // 60 * 60 * 60 * 60 * 1000);
	}
};


let sendAndAddToCache = (req, res, data) => {
	addToCache(req, data);
	res.send(data);
};

let checkCache = (req, res, cb) => {
	if (!useCache) return cb();
	let url = req.url + '?' + JSON.stringify(req.body) + JSON.stringify(req.params);
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

app.get('/api/portals/countries-stats', checkCache, (req, res) => {
	api.getCountriesStats((err, data) => {
		if (err) {
			console.log(err);
			return res.sendStatus(500);
		}
		sendAndAddToCache(req, res, {data: data});
	});
});


let registerCountryApi = country => {
	let api_path = '/api/' + (country.id || 'eu') + '/';
	let country_id = (country.id && (country.id !== 'eu') ? country.id.toUpperCase() : null);

	app.post(api_path + 'tender/search', checkCache, (req, res) => {
		api.searchTender(req.body, country_id, (err, data) => {
			if (err) {
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.post(api_path + 'company/search', checkCache, (req, res) => {
		api.searchCompany(req.body, country_id, (err, data) => {
			if (err) {
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.get(api_path + 'company/nuts', checkCache, (req, res) => {
		api.getCompanyNutsStats(country_id, (err, data) => {
			if (err) {
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.post(api_path + 'authority/search', checkCache, (req, res) => {
		api.searchAuthority(req.body, country_id, (err, data) => {
			if (err) {
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.get(api_path + 'authority/nuts', checkCache, (req, res) => {
		api.getAuthorityNutsStats(country_id, (err, data) => {
			if (err) {
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.get(api_path + 'sector/list/main', checkCache, (req, res) => {
		api.getCPVUsageStats(country_id, (err, data) => {
			if (err) {
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.post(api_path + 'autocomplete', checkCache, (req, res) => {
		api.autocomplete(req.body.entity, req.body.field, req.body.search, country_id, (err, data) => {
			if (err) {
				if (err === 404) {
					return res.sendStatus(404);
				}
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.post(api_path + 'market/stats', checkCache, (req, res) => {
		api.getMarketAnalysisStats(req.body, country_id, (err, data) => {
			if (err) {
				if (err === 404) {
					return res.sendStatus(404);
				}
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.post(api_path + 'home/stats', checkCache, (req, res) => {
		api.getHomeStats(country_id, (err, data) => {
			if (err) {
				if (err === 404) {
					return res.sendStatus(404);
				}
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.get(api_path + 'sector/id/:id', checkCache, (req, res) => {
		api.getCPV(req.params.id, (err, data) => {
			if (err) {
				if (err === 404) {
					return res.sendStatus(404);
				}
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.post(api_path + 'sector/stats', checkCache, (req, res) => {
		api.getCPVStats(req.body, country_id, (err, data) => {
			if (err) {
				if (err === 404) {
					return res.sendStatus(404);
				}
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.post(api_path + 'region/stats', checkCache, (req, res) => {
		api.getRegionStats(req.body, country_id, (err, data) => {
			if (err) {
				if (err === 404) {
					return res.sendStatus(404);
				}
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.get(api_path + 'tender/id/:id', checkCache, (req, res) => {
		api.getTender(req.params.id, (err, data) => {
			if (err) {
				if (err === 404) {
					return res.sendStatus(404);
				}
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.get(api_path + 'company/id/:id', checkCache, (req, res) => {
		api.getCompany(req.params.id, country_id, (err, data) => {
			if (err) {
				if (err === 404) {
					return res.sendStatus(404);
				}
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.post(api_path + 'company/stats', checkCache, (req, res) => {
		api.getCompanyStats(req.body, country_id, (err, data) => {
			if (err) {
				if (err === 404) {
					return res.sendStatus(404);
				}
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.post(api_path + 'company/nuts', checkCache, (req, res) => {
		api.getCompanyNutsStats(country_id, (err, data) => {
			if (err) {
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.post(api_path + 'indicators/stats', checkCache, (req, res) => {
		api.getIndicatorStats(req.body, country_id, (err, data) => {
			if (err) {
				if (err === 404) {
					return res.sendStatus(404);
				}
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.get(api_path + 'company/similar/:id', checkCache, (req, res) => {
		api.searchSimilarCompany(req.params.id, country_id, (err, data) => {
			if (err) {
				if (err === 404) {
					return res.sendStatus(404);
				}
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.get(api_path + 'authority/id/:id', checkCache, (req, res) => {
		api.getAuthority(req.params.id, country_id, (err, data) => {
			if (err) {
				if (err === 404) {
					return res.sendStatus(404);
				}
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.post(api_path + 'authority/stats', checkCache, (req, res) => {
		api.getAuthorityStats(req.body, country_id, (err, data) => {
			if (err) {
				if (err === 404) {
					return res.sendStatus(404);
				}
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.get(api_path + 'authority/id/:id', checkCache, (req, res) => {
		api.searchSimilarAuthority(req.params.id, country_id, (err, data) => {
			if (err) {
				if (err === 404) {
					return res.sendStatus(404);
				}
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.get(api_path + 'quality/usage', checkCache, (req, res) => {
		api.getFieldsUsage(country_id, (err, data) => {
			if (err) {
				if (err === 404) {
					return res.sendStatus(404);
				}
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.get(api_path + 'location/map.geojson', checkCache, (req, res) => {
		api.getLocationsMap((err, data) => {
			if (err) {
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});

	});

	app.get(api_path + 'company/similar/:id', checkCache, (req, res) => {
		api.searchSimilarCompany(req.params.id, country_id, (err, data) => {
			if (err) {
				if (err === 404) {
					return res.sendStatus(404);
				}
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

	app.get(api_path + 'authority/similar/:id', checkCache, (req, res) => {
		api.searchSimilarAuthority(req.params.id, country_id, (err, data) => {
			if (err) {
				if (err === 404) {
					return res.sendStatus(404);
				}
				console.log(err);
				return res.sendStatus(500);
			}
			sendAndAddToCache(req, res, {data: data});
		});
	});

};
portals.active.forEach(registerCountryApi);

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
					let portal = portals.active.find(portal => portal.id === key);
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
