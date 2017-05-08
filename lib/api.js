const fs = require('fs');
const async = require('async');
const Store = require('./store.js');

const Api = function (config) {
	let me = this;
	let cpv_list = JSON.parse(fs.readFileSync(config.data.path + '/cpvs.json').toString());
	let schema = JSON.parse(fs.readFileSync(config.data.shared + '/schema.json').toString());

	const constAggregations = {
		'authorities': {
			'nested': {
				'path': 'buyers'
			},
			'aggregations': {
				'authorities_nested': {
					'terms': {
						'field': 'buyers.groupId'
					},
					'aggregations': {
						'hits': {
							'top_hits': {
								'size': 1
							}
						}
					}
				}
			}
		},
		'companies': {
			'nested': {
				'path': 'lots.bids.bidders'
			},
			'aggregations': {
				'companies_nested': {
					'terms': {
						'field': 'lots.bids.bidders.groupId'
					},
					'aggregations': {
						'hits': {
							'top_hits': {
								'size': 1
							}
						}
					}
				}
			}
		},
		'currencies': {
			'terms': {
				'field': 'finalPrice.currency'
			},
			'aggregations': {
				'sum_price': {
					'sum': {
						'field': 'finalPrice.netAmount'
					}
				}
			}
		},
		'cpvs': {
			'nested': {
				'path': 'cpvs'
			},
			'aggregations': {
				'cpvs_filter': {
					'filter': {
						'query': {
							'bool': {
								'must': [
									{
										'term': {
											'cpvs.isMain': true
										}
									}
								]
							}
						}
					},
					'aggregations': {
						'maincpvs': {
							'terms': {
								'field': 'cpvs.code'
							}
						}
					}
				}
			}
		},
		'dates': {
			'nested': {
				'path': 'lots'
			},
			'aggregations': {
				'dates_nested': {
					'date_histogram': {'field': 'lots.awardDecisionDate', 'interval': 'year'}
				}
			}
		},
		'lots': {
			'nested': {
				'path': 'lots'
			},
			'aggregations': {
				'top_reverse_nested': {
					'reverse_nested': {}
				},
				'lotsbids': {
					'nested': {
						'path': 'lots.bids'
					},
					'aggregations': {
						'lotsbids_nested_filter': {
							'filter': {
								'term': {
									'lots.bids.isWinning': true
								}
							},
							'aggregations': {
								'lotsbids_nested': {
									'nested': {
										'path': 'lots.bids.bidders'
									},
								}
							}
						}
					}
				}
			}
		},
		'corruption': {
			'terms': {
				'field': 'indicators.type'
			}
		}
	};

	const cvpsAggregations = ['authorities', 'companies', 'currencies', 'cpvs', 'dates', 'lots'];
	const corruptionAggregations = ['authorities', 'companies', 'currencies', 'cpvs', 'dates', 'lots', 'corruption'];

	let cpvs = {};
	let cpvs_main = {};
	cpv_list.forEach(cpv => {
		let s = cpv.CODE.slice(0, 8);
		cpvs[s] = cpv;
		if (cpv.CODE.slice(2, 8) == '000000') {
			s = cpv.CODE.slice(0, 2);
			cpvs_main[s] = cpv;
		}
	});

	let nested_objects = ['lots.bids.bidders', 'lots.bids', 'lots.cpvs', 'lots', 'buyers', 'cpvs'];

	let schemaResolve = obj => {
		if (obj.$ref) {
			const r = obj.$ref.split('/')[2];
			if (schema.definitions[r]) {
				return schema.definitions[r];
			}
		}
		if (obj.items) obj.items = schemaResolve(obj.items);
		if (obj.properties) {
			Object.keys(obj.properties).forEach(key => {
				obj.properties[key] = schemaResolve(obj.properties[key]);
			});
		}
		return obj;
	};

	schema = schemaResolve(schema);
	Object.keys(schema.definitions).forEach(key => {
		schema.definitions[key] = schemaResolve(schema.definitions[key]);
	});
	let store = new Store(config);

	let parseStatsAggregations = (agg_ids, aggregations, tender_count) => {
		let stats = {};

		if (agg_ids.indexOf('companies') >= 0) {
			stats.suppliers = {
				count: aggregations.companies.doc_count,
				top10: []
			};
			aggregations.companies.companies_nested.buckets.forEach(bucket => {
				stats.suppliers.top10.push({
					value: bucket.doc_count,
					body: bucket.hits.hits.hits[0]._source
				});
			});
		}
		if (agg_ids.indexOf('authorities') >= 0) {
			stats.buyers = {
				count: aggregations.authorities.doc_count,
				top10: []
			};
			aggregations.authorities.authorities_nested.buckets.forEach(bucket => {
				stats.buyers.top10.push({
					value: bucket.doc_count,
					body: bucket.hits.hits.hits[0]._source
				});
			});
		}
		if (agg_ids.indexOf('cpvs') >= 0) {
			stats.cpvs = {};
			aggregations.cpvs.cpvs_filter.maincpvs.buckets.forEach(bucket => {
				stats.cpvs[bucket.key] = {
					name: getCPVName(bucket.key, 'EN'),
					value: bucket.doc_count
				};
			});
		}
		if (agg_ids.indexOf('dates') >= 0) {
			stats.lots_in_years = {};
			aggregations.dates.dates_nested.buckets.forEach(bucket => {
				let year = parseInt(bucket.key_as_string.slice(0, 4), 10);
				//TODO: make sure dates are valided on import & make valid date range a config entry
				if (year > 1999 && year < 2018)
					stats.lots_in_years[year] = bucket.doc_count;
			});
		}
		if (agg_ids.indexOf('currencies') >= 0) {
			stats.sum_price = {};
			aggregations.currencies.buckets.forEach(bucket => {
				stats.sum_price[bucket.key] = bucket.sum_price.value;
			});
		}
		if (agg_ids.indexOf('corruption') >= 0) {
			stats.corruption = {};
			aggregations.corruption.buckets.forEach(bucket => {
				stats.corruption[bucket.key] = bucket.doc_count;
			});
		}
		if (agg_ids.indexOf('lots') >= 0) {
			stats.counts = {
				bids: aggregations.lots.lotsbids.doc_count,
				bids_awarded: aggregations.lots.lotsbids.lotsbids_nested_filter.lotsbids_nested.doc_count,
				lots: aggregations.lots.doc_count,
				tenders: tender_count
			};
		}
		return stats;
	};

	let getAggregations = (agg_names) => {
		let result = {};
		agg_names.forEach(key => {
			result[key] = constAggregations[key];
		});
		return result;
	};

	let getCPVName = (id, lang) => {
		let cpv = cpvs_main[id];
		if (cpv && cpv[lang]) {
			return cpv[lang];
		}
		cpv = cpvs[id];
		if (cpv && cpv[lang]) {
			return cpv[lang];
		}
		return 'CPV#' + id;
	};

	let getNestedField = fieldname => nested_objects.filter(nested => {
		return fieldname.indexOf(nested) === 0;
	})[0];

	let buildSearchAggregations = options => {
		let result = null;
		let resolveAgg = (agg, node) => {
			if (!agg.field) return;

			let nested = getNestedField(agg.field);
			if (nested) {
				let aa = {
					'nested': {
						'path': nested
					},
					aggregations: {}
				};
				aa.aggregations[agg.field.replace(/\./g, '_') + '_nested'] = {
					'terms': {'field': agg.field, size: agg.size || 5}
				};
				node[agg.field.replace(/\./g, '_')] = aa;
				return;
			}

			if (agg.type == 'sum') {
				node[agg.field.replace(/\./g, '_') + '_sum'] = {'sum': {'field': agg.field}};
				return;
			} else if (agg.type == 'top') {
				node[agg.field.replace(/\./g, '_') + '_hits'] = {'top_hits': {'size': 1, _source: {include: [agg.field]}}};
				return;
			} else if (agg.type == 'histogram') {
				node[agg.field.replace(/\./g, '_') + '_over_time'] = {'date_histogram': {'field': agg.field, 'interval': 'year'}};
				return;
			} else if (agg.type == 'value') {
				return;
			}
			let aa = {'terms': {'field': agg.field, size: agg.size || 5}};
			if (agg.aggregations) {
				aa.aggregations = {};
				agg.aggregations.forEach(aagg => {
					resolveAgg(aagg, aa.aggregations);
				});
			}
			node[agg.field.replace(/\./g, '_')] = aa;
		};
		if (options.aggregations) {
			result = {};
			options.aggregations.forEach((agg) => {
				resolveAgg(agg, result);
			});
		}
		return result;
	};

	let buildSearchFilter = filter => {

		let buildSearchFilterInternal = f => {
			if (f.type == 'select') {
				let m = {terms: {}};
				m.terms[f.field] = f.value;
				return m;
			} else if (f.type == 'term') {
				let terms = Array.isArray(f.value) ? f.value : [f.value];
				if (terms.length > 1) {
					let b = {or: []};
					terms.forEach(v => {
						let m = {term: {}};
						m.term[f.field] = v;
						b.or.push(m);
					});
					return b;
				} else if (terms.length > 0) {
					let m = {term: {}};
					m.term[f.field] = terms[0];
					return m;
				}
			} else if (f.type == 'match') {
				let terms = Array.isArray(f.value) ? f.value : [f.value];
				if (terms.length > 1) {
					let b = {or: []};
					terms.forEach(v => {
						let m = {match: {}};
						m.match[f.field] = v;
						b.or.push(m);
					});
					return b;
				} else if (terms.length > 0) {
					let m = {match: {}};
					m.match[f.field] = terms[0];
					return m;
				}
			} else if (f.type == 'text') {
				if (!Array.isArray(f.value)) f.value = [f.value];
				return {
					'or': f.value.map((s) => {
						let m = {match_phrase_prefix: {}};
						m.match_phrase_prefix[f.field] = s;
						// m.slop = 10;
						return m;
					})
				};
			} else if (f.type == 'value') {
				let value = parseFloat(f.value[0]);
				if (isNaN(value)) return null;
				if (f.mode === '=') {
					let m = {term: {}};
					m.term[f.field] = value;
					return m;
				} else if (f.mode === '<') {
					let m = {range: {}};
					m.range[f.field] = {lt: value};
					return m;
				} else if (f.mode === '>') {
					let m = {range: {}};
					m.range[f.field] = {gt: value};
					return m;
				}
				return null;
			}
			return null;
		};

		let subfilters = [];

		if (filter.and) {
			subfilters = filter.and.map(and => {
				return buildSearchFilterInternal(and);
			});
		}
		let nested = getNestedField(filter.field);
		if (nested) {
			let f = {
				'nested': {
					'path': nested,
					'query': {
						'bool': {
							'must': [
								buildSearchFilterInternal(filter)
							].concat(subfilters)
						}
					}
				}
			};
			return f;
		}
		let result = buildSearchFilterInternal(filter);
		if (subfilters.length > 0) {
			result = {
				'bool': {
					'must': [
						result
					].concat(subfilters)
				}
			};
		}
		return result;
	};

	let buildSearchBody = options => {
		let body = {
			query: {},
			sort: {
				'modified': {
					'order': 'desc'
				}
			}
		};
		if (options.sort && options.sort.field) {
			body.sort = {};
			body.sort[options.sort.field] = {
				'order': options.sort.ascend ? 'asc' : 'desc'
			};
		}
		let aggregations = buildSearchAggregations(options);
		if (aggregations) {
			body.aggregations = aggregations;
		}
		if (options.filters && options.filters.length > 0) {
			let filters = [];
			options.filters.forEach(filter => {
				if (filter.type == 'date') {
					body.sort = {};
					body.sort[filter.field] = {
						'order': 'desc'
					};
				} else {
					let f = buildSearchFilter(filter);
					if (f) filters.push(f);
				}
			});
			if (filters.length > 0) {
				body.query = {
					'filtered': {
						'query': {
							'match_all': {}
						},
						'filter': {}
					}
				};
				if (filters.length > 1) {
					body.query.filtered.filter = {bool: {must: filters}};
				} else {
					body.query.filtered.filter = filters[0];
				}
			}
		}
		if (Object.keys(body.query).length == 0) {
			body.query.match_all = {};
		}
		return body;
	};

	let compactAggregations = aggregations => {

		let resolveNode = n => {
			if (!n) return;
			Object.keys(n).forEach(key => {
				let o = n[key];
				if (key !== 'buckets' && typeof o === 'object') {
					resolveNode(o);
					if (key.indexOf('_nested') > 0) {
						Object.keys(o).forEach(k => {
							n[k] = o[k];
						});
						n[key] = undefined;
					}
				}

			});
		};
		resolveNode(aggregations);
	};

	let buildCountrySearchBody = (options, country_id) => {
		let body = buildSearchBody(options);
		if (country_id) {
			if (body.query.match_all) {
				body.query = {
					term: {
						'country': country_id
					}
				};
			} else if (body.query.filtered.query.match_all) {
				body.query.filtered.query = {
					term: {
						'country': country_id
					}
				};
			} else {
				console.log('unknown search body format', body);
			}
		}
		return body;
	};

	let searchCompanies = (body, size, from, cb) => {
		body.sort = undefined;
		body.aggregations['companies'] = {
			'terms': {
				'field': 'body.name.raw',
				'size': 1000
			},
			'aggregations': {
				'hits': {
					'top_hits': {
						'size': 1
					}
				}
			}
		};
		store.Company.search(body, 0, 0, (err, result) => {
			if (err) {
				return cb(err);
			}
			result.hits = {total: 0, hits: []};
			if (result.aggregations && result.aggregations.companies) {
				result.aggregations.companies.buckets.slice(from, from + size).forEach(bucket => {
					bucket.hits.hits.hits[0]._source.value = bucket.doc_count;
					result.hits.hits.push(bucket.hits.hits.hits[0]._source);
				});
				result.hits.total = result.aggregations.companies.buckets.length;
				result.aggregations.companies = undefined;
			}
			cb(null, {hits: result.hits, aggregations: result.aggregations});
		});
	};

	let searchAuthorities = (body, size, from, cb) => {
		body.sort = undefined;
		body.aggregations['authorities'] = {
			'terms': {
				'field': 'body.name.raw',
				'size': 1000
			},
			'aggregations': {
				'hits': {
					'top_hits': {
						'size': 1
					}
				}
			}
		};
		store.Authority.search(body, 0, 0, (err, result) => {
			if (err) {
				return cb(err);
			}
			result.hits = {total: 0, hits: []};
			if (result.aggregations && result.aggregations.authorities) {
				result.aggregations.authorities.buckets.slice(from, from + size).forEach(bucket => {
					bucket.hits.hits.hits[0]._source.value = bucket.doc_count;
					result.hits.hits.push(bucket.hits.hits.hits[0]._source);
				});
				result.hits.total = result.aggregations.authorities.buckets.length;
				result.aggregations.authorities = undefined;
			}
			cb(null, {hits: result.hits, aggregations: result.aggregations});
		});
	};

	let getCPVMain = (country_id, withStats, cb) => {

		let b = {
			'aggregations': {
				'cpvs': {
					'nested': {
						'path': 'cpvs'
					},
					'aggregations': {
						'cpvs_filter': {
							'filter': {
								'query': {
									'term': {
										'cpvs.isMain': true
									}
								}
							},
							'aggregations': {
								'maincpvs': {
									'terms': {
										'field': 'cpvs.code.main'
									}
								}
							}
						}
					}
				}
			}
		};
		if (withStats) {
			b.aggregations.cpvs.aggregations.cpvs_filter.aggregations.maincpvs.aggregations = {
				'tender_denested': {
					'reverse_nested': {},
					'aggregations': getAggregations(cvpsAggregations)
				}
			};
		}

		if (country_id) {
			b.query = {
				term: {
					'country': country_id
				}
			};
		}

		store.Tender.search(b, 0, 0, (err, data) => {
			if (err) {
				console.log(err);
				return cb(err);
			}

			let result = data.aggregations.cpvs.cpvs_filter.maincpvs.buckets.map(bucket => {
				// console.log(JSON.stringify(bucket, null, '\t'));
				let sector = {
					id: bucket.key,
					name: cpvs_main[bucket.key]['EN'],
					value: bucket.doc_count
				};
				if (!withStats) {
					return sector;
				}
				let entry = {
					sector: sector,
					stats: parseStatsAggregations(cvpsAggregations, bucket.tender_denested, bucket.tender_denested.doc_count)
				};
				// console.log(JSON.stringify(entry, null, '\t'));
				return entry;
			});
			cb(null, result);
		});
	};

	let getCPVMainStats = (country_id, cb) => {
		getCPVMain(country_id, true, cb);
	};

	this.getCountriesStats = cb => {
		let body = {
			query: {
				match_all: {}
			},
			aggregations: {
				countries: {
					terms: {
						field: 'country',
						size: 10000
					}
				}
			}
		};
		store.Tender.search(body, 0, 0, (err, data) => {
			if (err) return cb(err);
			let countries = {};
			data.aggregations.countries.buckets.forEach((bucket) => {
				countries[bucket.key.toLowerCase()] = bucket.doc_count;
			});
			cb(null, countries);
		});
	};

	this.getUsage = (country_id, cb) => {
		let aggregations = {};
		let ids = {};

		let scanSchema = (p, obj) => {
			if (!obj.properties) return;
			Object.keys(obj.properties).forEach(key => {
				let field = p.concat([key]).join('.');
				let nested = getNestedField(field);
				let aggs = aggregations;
				if (nested) {
					let nested_id = nested.replace(/\./g, '_') + '#nested';
					if (!aggregations[nested_id]) {
						aggregations[nested_id] = {
							'nested': {
								'path': nested
							},
							'aggregations': {}
						};
					}
					aggs = aggregations[nested_id].aggregations;
				}
				let id = field.replace(/\./g, '_');
				let prop = obj.properties[key];
				prop.field = field;
				ids[id] = prop;
				aggs[id + '#available'] = {'filter': {'not': {'missing': {'existence': true, 'field': field}}}};
				aggs[id + '#missing'] = {'filter': {'missing': {'existence': true, 'field': field}}};
				if (prop.distinct) {
					aggs[id + '#distinct'] = {'cardinality': {'field': field}};
				}
				if (prop.type == 'object') {
					scanSchema(p.concat([key]), prop);
				} else if (prop.type == 'array') {
					scanSchema(p.concat([key]), prop.items);
				}
			});
		};

		scanSchema([], schema.definitions.tender);

		store.Tender.aggregations(country_id, aggregations, (err, response) => {
			if (err) {
				return cb(err);
			}
			let results = {};

			let resolveResult = (key, result) => {
				// console.log(key, result);
				let parts = key.split('#');
				let id = parts[0];
				let value = parts[1];

				if (value === 'nested') {
					Object.keys(result).forEach(subkey => {
						if (['doc_count', 'value'].indexOf(subkey) < 0) {
							resolveResult(subkey, result[subkey]);
						}
					});
				} else {
					results[id] = results[id] || {field: id.replace(/_/g, '.')};
					ids[id].usage = ids[id].usage || {};
					if (value == 'distinct') {
						ids[id].usage[value] = result.value;
						results[id][value] = result.value;
					}
					else {
						ids[id].usage[value] = result.doc_count;
						results[id][value] = result.doc_count;
					}
				}

			};

			Object.keys(response.aggregations).forEach(key => {
				resolveResult(key, response.aggregations[key]);
			});

			cb(null,
				Object.keys(results).map(key => results[key]).sort((a, b) => {
					if (a.field < b.field) return -1;
					if (a.field > b.field) return 1;
					return 0;
				})
			);
		});
	};

	this.searchSimilarCompanyName = (name, ignoreIds, country_id, cb) => {
		let body = {
			'query': {
				'bool': {
					'must': [
						{
							'match_phrase': {
								'body.name.slug': name
							}
						}
					]
				}
			},
			'aggregations': {
				'like_slugs': {
					'terms': {
						'field': 'body.groupId'
					},
					'aggregations': {
						'hits': {
							'top_hits': {
								'size': 1
							}
						}
					}
				}
			}
		};
		if (ignoreIds && ignoreIds.length > 0) {
			body.query.bool.must_not = {
				terms: {'body.groupId': ignoreIds}
			};
		}
		if (country_id) {
			body.query.bool.must.push({'term': {'country': country_id}});
		}
		store.Company.search(body, 10000, 0, (err, results) => {
			if (err) return cb(err);
			cb(null,
				{
					similar: results.aggregations.like_slugs.buckets.map(bucket => {
						return {value: bucket.doc_count, body: bucket.hits.hits.hits[0]._source.body, country: bucket.hits.hits.hits[0]._source.country};
					})
				}
			);
		});
	};

	this.searchSimilarAuthorityName = function (name, ignoreIds, country_id, cb) {
		let body = {
			'query': {
				'bool': {
					'must': [
						{
							'match_phrase': {
								'body.name.slug': name
							}
						}
					]
				}
			},
			'aggregations': {
				'like_slugs': {
					'terms': {
						'field': 'body.groupId'
					},
					'aggregations': {
						'hits': {
							'top_hits': {
								'size': 1
							}
						}
					}
				}
			}
		};

		if (ignoreIds && ignoreIds.length > 0) {
			body.query.bool.must_not = {
				terms: {'body.groupId': ignoreIds}
			};
		}

		if (country_id) {
			body.query.bool.must.push({'term': {'country': country_id}});
		}
		store.Authority.search(body, 10000, 0, (err, results) => {
			if (err) return cb(err);
			cb(null,
				{
					similar: results.aggregations.like_slugs.buckets.map(bucket => {
						return {value: bucket.doc_count, body: bucket.hits.hits.hits[0]._source.body, country: bucket.hits.hits.hits[0]._source.country};
					})
				}
			);
		});
	};

	this.getViz = (ids, country_id, cb) => {
		if ((typeof ids !== 'string') || ( ids.trim() === '')) return cb(404);
		let viz = ids.split(',');
		let result = {};
		async.forEachSeries(viz, (v, next) => {
			if (result[v]) {
				return next();
			}
			if (v == 'sectors_count') {
				me.getCPVMainUsage(country_id, (err, data) => {
					if (err) return cb(err);
					result[v] = data;
					next();
				});
			} else if (v == 'sectors_stats') {
				getCPVMainStats(country_id, (err, data) => {
					if (err) return cb(err);
					result[v] = data;
					next();
				});
			} else if (v == 'corruption_indicators') {
				me.getCorruptionIndicatorStats(country_id, (err, data) => {
					if (err) return cb(err);
					result[v] = data;
					next();
				});
			} else {
				next();
			}
		}, (err) => {
			cb(err, result);
		});

	};

	this.getTender = (id, cb) => {
		if ((typeof id !== 'string') || ( id.trim() === '')) return cb(404);
		store.Tender.get(id, (err, result) => {
			if (err || !result) {
				cb(404);
			} else {
				if (result.hits.total > 1) {
					console.log('warning', 'multiple tender found for one id', id);
				}
				if (result.hits.total < 1) {
					cb(404);
				} else {
					cb(null, result.hits.hits[0]._source);
				}
			}
		});
	};

	this.getCompany = (id, country_id, cb) => {
		if ((typeof id !== 'string') || ( id.trim() === '')) return cb(404);
		store.Company.get(id, (err, result) => {
			if (err) return cb(err);
			if (result.hits.hits.length === 0) return cb(404);
			cb(null, {company: result.hits.hits[0]._source});
		});
	};

	this.getCompanyStats = (ids, country_id, cb) => {
		if (!Array.isArray(ids)) ids = [ids];
		ids = ids.filter(id => {
			return (typeof id == 'string') && (id.trim().length > 0);
		});
		if (ids.length === 0) return cb(404);
		let b = {
			'query': {
				'bool': {
					'must': [
						{
							'nested': {
								'path': 'lots.bids',
								'query': {
									'bool': {
										'must': [
											{
												'nested': {
													'path': 'lots.bids.bidders',
													'query': {
														'terms': {
															'lots.bids.bidders.groupId': ids
														}
													}

												}
											}
										]
									}
								}
							}
						}
					]
				}
			},
			'aggregations': {
				'lotsbids': {
					'nested': {
						'path': 'lots.bids'
					},
					'aggregations': {
						'lotsbids_nested_filter': {
							'filter': {
								'term': {
									'lots.bids.isWinning': true
								}
							},
							'aggregations': {
								'lotsbids_nested': {
									'nested': {
										'path': 'lots.bids.bidders'
									},
									'aggregations': {
										'lotsbids_nested_filter': {
											'filter': {
												'terms': {
													'lots.bids.bidders.groupId': ids
												}
											},
											'aggregations': {
												'lotsbids_denested': {
													'reverse_nested': {
														'path': 'lots.bids'
													},
													'aggregations': {
														'currencies': {
															'terms': {
																'field': 'lots.bids.price.currency'
															},
															'aggregations': {
																'sum_price': {
																	'sum': {
																		'field': 'lots.bids.price.netAmount'
																	}
																}
															}
														}
													}
												},
												'lots_denested': {
													'reverse_nested': {
														'path': 'lots'
													},
													'aggregations': {
														'dates': {
															'date_histogram': {'field': 'lots.awardDecisionDate', 'interval': 'year'}
														}
													}
												},
												'tender_denested': {
													'reverse_nested': {},
													'aggregations': {
														'authorities': constAggregations.authorities
													}
												}
											}
										}
									}
								}
							}
						}
					}
				},
				'lotsbids_count': {
					'nested': {
						'path': 'lots.bids'
					},
					'aggregations': {
						'lotsbids_nested': {
							'nested': {
								'path': 'lots.bids.bidders'
							},
							'aggregations': {
								'lotsbids_nested_filter': {
									'filter': {
										'terms': {
											'lots.bids.bidders.groupId': ids
										}
									},
									'aggregations': {
										'lots_denested': {
											'reverse_nested': {
												'path': 'lots'
											}
										},
										'tender_denested': {
											'reverse_nested': {}
										}
									}
								}
							}
						}
					}
				},
				'cpvs': constAggregations.cpvs
			}
		};
		if (country_id) {
			b.query.bool.must.push({
				term: {
					'country': country_id
				}
			});
		}
		store.Tender.search(b, 0, 0, (err, result) => {
			if (err) {
				return cb(err);
			}
			let stats = {};
			stats.cpvs = {};
			result.aggregations.cpvs.cpvs_filter.maincpvs.buckets.forEach(bucket => {
				stats.cpvs[bucket.key] = {
					name: getCPVName(bucket.key, 'EN'),
					value: bucket.doc_count
				};
			});
			stats.sum_price = {};
			result.aggregations.lotsbids.lotsbids_nested_filter.lotsbids_nested.lotsbids_nested_filter.lotsbids_denested.currencies.buckets.forEach(bucket => {
				stats.sum_price[bucket.key] = bucket.sum_price.value;
			});
			stats.bids_in_years = {};
			result.aggregations.lotsbids.lotsbids_nested_filter.lotsbids_nested.lotsbids_nested_filter.lots_denested.dates.buckets.forEach(bucket => {
				stats.bids_in_years[bucket.key_as_string.slice(0, 4)] = bucket.doc_count;
			});
			stats.counts = {
				bids: result.aggregations.lotsbids_count.lotsbids_nested.lotsbids_nested_filter.doc_count,
				bids_awarded: result.aggregations.lotsbids.lotsbids_nested_filter.lotsbids_nested.lotsbids_nested_filter.doc_count,
				lots: result.aggregations.lotsbids_count.lotsbids_nested.lotsbids_nested_filter.lots_denested.doc_count,
				tenders: result.aggregations.lotsbids_count.lotsbids_nested.lotsbids_nested_filter.tender_denested.doc_count
			};
			stats.buyers = {top10: []};
			result.aggregations.lotsbids.lotsbids_nested_filter.lotsbids_nested.lotsbids_nested_filter.tender_denested.authorities.authorities_nested.buckets.forEach(bucket => {
				stats.buyers.top10.push({
					value: bucket.doc_count,
					body: bucket.hits.hits.hits[0]._source
				});
			});
			cb(null, {stats: stats});
		});
	};

	this.searchSimilarCompany = (id, country_id, cb) => {
		if ((typeof id !== 'string') || ( id.trim() === '')) return cb(404);
		store.Company.get(id, (err, result) => {
			if (err) return cb(err);
			if (result.hits.hits.length === 0) return cb(404);
			let company = result.hits.hits[0]._source;
			this.searchSimilarCompanyName(company.body.name, [id], country_id, cb);
		});
	};

	this.searchSimilarAuthority = (id, country_id, cb) => {
		if ((typeof id !== 'string') || ( id.trim() === '')) return cb(404);
		store.Authority.get(id, (err, result) => {
			if (err) return cb(err);
			if (result.hits.hits.length === 0) return cb(404);
			let company = result.hits.hits[0]._source;
			this.searchSimilarAuthorityName(company.body.name, [id], country_id, cb);
		});
	};

	this.getAuthority = (id, country_id, cb) => {
		if ((typeof id !== 'string') || (id.trim() === '')) return cb(404);
		store.Authority.get(id, (err, result) => {
			if (err) return cb(err);
			if (result.hits.hits.length === 0) return cb(404);
			cb(null, {authority: result.hits.hits[0]._source});
		});
	};

	this.getAuthorityStats = (ids, country_id, cb) => {
		if (!Array.isArray(ids)) ids = [ids];
		ids = ids.filter(id => {
			return (typeof id == 'string') && (id.trim().length > 0);
		});
		if (ids.length === 0) return cb(404);
		let b = {
			'query': {
				'bool': {
					'must': [
						{
							'nested': {
								'path': 'buyers',
								'query': {
									'bool': {
										'must': [
											{
												'terms': {
													'buyers.groupId': ids
												}
											}
										]
									}
								}
							}
						}
					]
				}
			},
			'aggregations': {
				'lotsbids': {
					'nested': {
						'path': 'lots.bids'
					},
					'aggregations': {
						'lotsbids-filter': {
							'filter': {
								'query': {
									'bool': {
										'must': [
											{
												'term': {
													'lots.bids.isWinning': true
												}
											}
										]
									}
								}
							},
							'aggregations': {
								'suppliers': {
									'nested': {
										'path': 'lots.bids.bidders'
									},
									'aggregations': {
										'suppliers_nested': {
											'terms': {
												'field': 'lots.bids.bidders.groupId',
												'size': 10
											},
											'aggregations': {
												'hits': {
													'top_hits': {
														'size': 1
													}
												}
											}
										}
									}
								}
							}
						}
					}
				},
				'cpvs': {
					'nested': {
						'path': 'cpvs'
					},
					'aggregations': {
						'cpvs-filter': {
							'filter': {
								'query': {
									'bool': {
										'must': [
											{
												'term': {
													'cpvs.isMain': true
												}
											}
										]
									}
								}
							},
							'aggregations': {
								'maincpvs': {
									'terms': {
										'field': 'cpvs.code.main'
									}
								}
							}
						}
					}
				},
				'currencies': {
					'terms': {
						'field': 'finalPrice.currency'
					},
					'aggregations': {
						'sum_price': {
							'sum': {
								'field': 'finalPrice.netAmount'
							}
						}
					}
				},
				'dates': {
					'nested': {
						'path': 'lots'
					},
					'aggregations': {
						'dates_nested': {
							'date_histogram': {'field': 'lots.awardDecisionDate', 'interval': 'year'}
						}
					}
				},
				'lots': {
					'nested': {
						'path': 'lots'
					},
					'aggregations': {
						'top_reverse_nested': {
							'reverse_nested': {}
						}
					}
				}
			}
		};
		if (country_id) {
			b.query.bool.must.push({
				term: {
					'country': country_id
				}
			});
		}
		store.Tender.search(b, 0, 0, (err, result) => {
			if (err) {
				console.log(err);
				return cb(err);
			}
			let stats = {};
			stats.cpvs = {};
			result.aggregations.cpvs['cpvs-filter'].maincpvs.buckets.forEach(bucket => {
				stats.cpvs[bucket.key] = {
					name: getCPVName(bucket.key, 'EN'),
					value: bucket.doc_count
				};
			});
			stats.sum_price = {};
			result.aggregations.currencies.buckets.forEach(bucket => {
				stats.sum_price[bucket.key] = bucket.sum_price.value;
			});
			stats.bids_in_years = {};
			result.aggregations.dates.dates_nested.buckets.forEach(bucket => {
				stats.bids_in_years[bucket.key_as_string.slice(0, 4)] = bucket.doc_count;
			});
			stats.counts = {
				bids: result.aggregations.lotsbids.doc_count,
				bids_awarded: result.aggregations.lotsbids['lotsbids-filter'].doc_count,
				lots: result.aggregations.lots.doc_count,
				tenders: result.aggregations.lots.top_reverse_nested.doc_count
			};
			stats.suppliers = {top10: []};
			result.aggregations.lotsbids['lotsbids-filter'].suppliers.suppliers_nested.buckets.forEach(bucket => {
				stats.suppliers.top10.push({
					value: bucket.doc_count,
					body: bucket.hits.hits.hits[0]._source
				});
			});
			cb(null, {stats: stats});
		});
	};

	this.searchAuthority = (options, country_id, cb) => {
		let body = buildCountrySearchBody(options, country_id);
		searchAuthorities(body, (options.size || 10), (options.from || 0), cb);
	};

	this.searchCompany = (options, country_id, cb) => {
		let body = buildCountrySearchBody(options, country_id);
		searchCompanies(body, (options.size || 10), (options.from || 0), cb);
	};

	this.searchTender = (options, country_id, cb) => {
		let body = buildCountrySearchBody(options, country_id);
		store.Tender.search(body, (options.size || 10), (options.from || 0), (err, result) => {
			if (err) {
				return cb(err);
			}
			if (body.sort) {
				let key = Object.keys(body.sort)[0];
				result.sortBy = {id: key, ascend: body.sort[key].order === 'asc'};
			}
			compactAggregations(result.aggregations);
			result.hits.hits = result.hits.hits.map(doc => doc._source);
			cb(null, result);
		});
	};

	this.autocomplete = (entity, field, search, country_id, cb) => {
		// console.log('autocomplete', entity, field, search, country_id);
		let index;
		if (entity == 'tender') {
			index = store.Tender;
		} else if (entity == 'company') {
			index = store.Company;
		} else if (entity == 'authority') {
			index = store.Authority;
		} else {
			return cb(null, []);
		}

		let nested = getNestedField(field);

		let queryfield = null;
		let queryfield_pure = null;
		if (search && search.length > 0) {
			queryfield_pure = {
				match_phrase_prefix: {}
			};
			queryfield_pure.match_phrase_prefix[field] = search;
			if (nested) {
				queryfield = {
					'nested': {
						'path': nested,
						query: queryfield_pure
					}
				};
			} else {
				queryfield = queryfield_pure;
			}
		}

		let aggfield = {
			'result': {
				terms: {
					field: field + '.raw',
					size: 100
				}

			}
		};
		if (nested) {
			aggfield = {
				nestedresult: {
					'nested': {
						'path': nested
					},
					'aggregations': {
						'nested-filter': {
							'filter': {
								'query': {
									'bool': {
										'must': [
											queryfield_pure
										]
									}
								}
							},
							'aggregations': aggfield
						}
					}
				}
			};
		}

		let body = {
			query: {
				'filtered': {
					'query': {
						'match_all': {}
					},
					'filter': {
						bool: {
							must: [
								{
									term: {
										'country': country_id
									}
								}
							]
						}
					}
				}
			},
			aggregations: aggfield
		};
		if (queryfield) {
			body.query.filtered.filter.bool.must.push(queryfield);
		}
		// console.log(JSON.stringify(body, null, '\t'));
		index.search(body, 0, 0, (err, result) => {
			if (err) {
				return cb(err);
			}
			result = result.aggregations.nestedresult ? result.aggregations.nestedresult['nested-filter'].result : result.aggregations.result;
			cb(null, result.buckets.map((item) => {
				return {key: item.key, value: item.doc_count};
			}));
		});

	};

	this.getCPVMainUsage = (country_id, cb) => {
		getCPVMain(country_id, false, cb);
	};

	this.getCorruptionIndicatorStats = (country_id, cb) => {
		let b = {
			'query': {
				'bool': {
					'must': [
						{
							'match': {
								'indicators.type': 'CORRUPTION_SINGLE_BID'
							}
						}
					]
				}
			},
			'aggregations': getAggregations(corruptionAggregations)
		};
		if (country_id) {
			b.query.bool.must.push({
				term: {
					'country': country_id
				}
			});
		}

		store.Tender.search(b, 0, 0, (err, result) => {
			if (err) {
				console.log(err);
				return cb(err);
			}
			let stats = parseStatsAggregations(corruptionAggregations, result.aggregations, result.hits.total);
			cb(null, stats);
		});
	};

	this.getCPVStats = (id, lang, country_id, cb) => {

		if ((typeof id !== 'string') || ( id.trim() === '')) return cb(404);
		id = id.trim();
		let cpv = cpvs_main[id];
		let parent_cpv = id.length > 2 ? cpvs_main[id.slice(0, 2)] : null;
		if (!cpv) {
			cpv = cpvs[id];
		}
		if (!cpv) {
			return cb(404);
		}
		lang = (lang || 'EN').toUpperCase();
		if (!cpv[lang]) lang = 'EN';

		let term = id.length === 2 ? {'term': {'cpvs.code.main': id}} : {'term': {'cpvs.code': id}};

		let b = {
			'query': {
				'bool': {
					'must': [
						{
							'nested': {
								'path': 'cpvs',
								'query': {
									'bool': {
										'must': [
											term,
											{
												'term': {
													'cpvs.isMain': true
												}
											}
										]
									}
								}
							}
						}
					]
				}
			},
			'aggregations': getAggregations(cvpsAggregations)
		};
		if (country_id) {
			b.query.bool.must.push({
				term: {
					'country': country_id
				}
			});
		}
		store.Tender.search(b, 0, 0, (err, result) => {
			if (err) {
				return cb(err);
			}
			let stats = parseStatsAggregations(cvpsAggregations, result.aggregations, result.hits.total);
			if (parent_cpv) {
				parent_cpv = {id: id.slice(0, 2), name: parent_cpv[lang]};
			}
			cb(null, {sector: {id: id, name: cpv[lang]}, parent: parent_cpv, stats: stats});
		});

	};

	this.init = cb => {
		store.init(cb);
		// this.autocomplete('tender', 'lots.bids.bidders.name', 'Schlingmann Feuerwehrfahrzeuge', 'DE', (err, data) => {
		// 	console.log('done', err, JSON.stringify(data, null, '\t'));
		// });
		// this.getCorruptionIndicatorStats('DE', (err, data) => {
		// 	console.log('done', err, JSON.stringify(data, null, '\t'));
		// });
	};


};

module.exports = Api;