#!/usr/bin/env node

/*

 Downloads/Scrapes public body lists from FOI portals

 */

const fs = require('fs');
const path = require('path');
const async = require('async');
const scrapyard = require('scrapyard');
const config = require('../config.js');
const portals = require(path.join(config.data.shared, 'portals.json')).active.filter(portal => portal.foi !== undefined);
const mkdirp = require('mkdirp');

let scraper = new scrapyard({
	debug: true,
	retries: 5,
	connections: 10,
	cache: path.join(config.data.path, './scraper_storage'),
	bestbefore: '5000min'
});

let requestBodies = (portal, next) => {

	let transform = o => ({
		id: o.slug,
		name: o.name
	});

	let objs = [];
	let requestBodiesLimit = (offset, cb) => {
		const limit = 100;
		const url = portal.foi.url + '/api/v1/publicbody/?limit=' + limit + '&offset=' + offset;
		console.log(url);
		scraper({
			url: url,
			type: 'json'
		}, (err, json) => {
			if (err) {
				return console.error(err);
			}
			let info = json;
			if (info.objects.length < 1) {
				next(null, objs);
			} else {
				objs = objs.concat(info.objects.map(transform));
				if (objs.length < info.meta.total_count) {
					requestBodiesLimit(offset + limit, cb);
				} else {
					next(null, objs);
				}
			}
		});
	};

	let scrapeBodies = (page, cb) => {
		const url = portal.foi.url + '/body?page=' + page;
		console.log(url);
		scraper(url, (err, $) => {
			if (err) {
				return console.error(err);
			}
			let count = 0;
			$('.body_listing a ').each((index, elem) => {
				let o = {
					id: $(elem).attr('href').replace('/body/', ''),
					name: $(elem).text().trim()
				};
				count++;
				objs.push(o);
			});
			if (count > 0) {
				scrapeBodies(page + 1, cb);
			} else {
				cb();
			}
		});
	};
	if (portal.foi.format === 'froide') {
		requestBodiesLimit(0, () => {
			next(null, objs);
		});
	} else if (portal.foi.format === 'alaveteli') {
		scrapeBodies(1, () => {
			next(null, objs);
		});
	}
};

let download = (portal, cb) => {
	const filename = path.resolve(config.data.path, 'publicbodies', portal.id.toLowerCase() + '_publicbodies.json');
	requestBodies(portal, (err, objs) => {
		if (err) {
			return cb(err);
		}
		if (!objs) {
			return cb();
		}
		fs.writeFileSync(filename, JSON.stringify(objs));
		cb();
	});
};

mkdirp(path.resolve(config.data.path, 'publicbodies'), err => {
	if (err) {
		return console.error(err);
	}
	async.forEachSeries(portals, download, (err) => {
		if (err) {
			console.log('error', err);
		} else {
			console.log('done');
		}
	});
});

