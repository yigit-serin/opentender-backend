#!/usr/bin/env node

/*

 Requests some long running aggregations, so they are cached

 */

const fs = require('fs');
const path = require('path');
const async = require('async');
const request = require('request');
const config = require('../config.js');

let portals = JSON.parse(fs.readFileSync(path.join(config.data.shared, 'portals.json')).toString()).active;

let warm_viz = (viz, cb) => {
	async.forEachSeries(portals, (portal, next) => {
		let url = 'http://' + config.listen.host + ':' + config.listen.port + '/api/' + portal.id.toLowerCase() + '/viz/ids/' + viz;
		console.log(url);
		request(url, (error, response, body) => {
			if (error) console.log(error);
			next();
		});
	}, cb);
};

warm_viz('sectors_stats', () => {
	warm_viz('corruption_indicators', () => {
		console.log('done');
	});
});
