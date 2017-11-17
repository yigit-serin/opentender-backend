#!/usr/bin/env node

const fs = require('fs');
const express = require('express');
const path = require('path');
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
	let id = (feature.properties.wb_a2 || '').toLowerCase();
	let p = portals.find(portal => {
		return portal.id === id;
	});
	return p;
}).map(feature => {
	return {
		type: feature.type,
		geometry: feature.geometry,
		properties: {id: feature.properties.wb_a2.toLowerCase()}
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

morgan.token('cached', (req) => {
	return req.cached ? 'true' : 'false';
});

app.use(morgan('[:date[clf]] - cached: :cached - :method :url - :res[content-length] - :response-time ms'));

let md5hash = (value) => {
	return crypto.createHash('md5').update(value).digest('hex');
};

let getCacheKey = (req) => {
	let key = JSON.stringify({u: req.originalUrl, b: req.body, p: req.params});
	return md5hash(key);
};

let addToCache = (req, data) => {
	cache.upsert(getCacheKey(req), data, (err, stored) => {
		if (err) {
			return console.error(error);
		}
	});
};

let sendAndAddToCache = (req, res, data) => {
	addToCache(req, data);
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

let processAnswer = (req, res, err, data, nocache) => {
	if (err) {
		console.log(err);
		if (err === 404) {
			return res.sendStatus(404);
		}
		return res.sendStatus(500);
	}
	if (nocache) {
		res.send({data: data});
	} else {
		sendAndAddToCache(req, res, {data: data});
	}
};

app.get('/api/portals/list', (req, res) => {
	res.send({data: portals});
});

app.get('/api/portals/geo.json', (req, res) => {
	res.send(portals_geojson);
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
			processAnswer(req, res, err, {used, unused});
		}
	});
});

let downloads = {};

let requestDownload = (body) => {
	const sbody = JSON.stringify(body);
	const id = md5hash(sbody);
	downloads[id] = sbody;
	return id;
};

let registerCountryApi = country => {
	let api_path = '/api/' + (country.id || 'eu') + '/';
	let country_id = (country.id && (country.id !== 'eu') ? country.id.toUpperCase() : null);

	app.post(api_path + 'tender/search', checkCache, (req, res) => {
		api.searchTender(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'download/id/:id', (req, res) => {
		let sbody = downloads[req.params.id];
		if (!sbody) {
			return res.send(404);
		}
		let body = JSON.parse(sbody);
		downloads[req.params.id] = undefined;
		api.streamTender(req.params.id, req, res, body, country_id);
	});

	app.post(api_path + 'tender/download', (req, res) => {
		processAnswer(req, res, null, {id: requestDownload(req.body)}, true);
	});

	app.post(api_path + 'company/search', checkCache, (req, res) => {
		api.searchSupplier(req.body, country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.get(api_path + 'company/nuts', checkCache, (req, res) => {
		api.getCompanyNutsStats(country_id, (err, data) => {
			processAnswer(req, res, err, data);
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

	app.get(api_path + 'sector/list/main', checkCache, (req, res) => {
		api.getCPVUsageStats(country_id, (err, data) => {
			processAnswer(req, res, err, data);
		});
	});

	app.post(api_path + 'autocomplete', (req, res) => {
		api.autocomplete(req.body.entity, req.body.field, req.body.search, country_id, (err, data) => {
			processAnswer(req, res, err, data, true);
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
	});
});
