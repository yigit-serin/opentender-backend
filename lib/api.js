const fs = require('fs');
const async = require('async');
const Store = require('./store.js');
const Library = require('./library');
const Queries = require('./queries');
const Utils = require('./utils');

const Api = function (config) {
	let library = new Library(config);
	let store = new Store(config);

	/**
	 tender dbs aggregations
	 */

	this.getAggregations = (country_id, aggregation_ids, cb) => {
		let aggs = Queries.buildAggregations(aggregation_ids);
		store.Tender.aggregations(country_id, aggs.request, (err, result) => {
			if (err) {
				console.log(err);
				return cb(err);
			}
			let stats = aggs.parse(result.aggregations, library, result.hits.total);
			cb(null, stats);
		});
	};

	this.getAggregation = (country_id, aggregation_id, cb) => {
		this.getAggregations(country_id, [aggregation_id], (err, stats) => {
			if (err) return cb(err);
			cb(null, stats[aggregation_id]);
		});
	};

	this.getIndicatorStats = (options, country_id, cb) => {
		let aggs = Queries.buildAggregations([
			'top_authorities',
			'top_companies',
			'sums_finalPrice',
			'terms_main_cpv_divisions',
			'terms_indicators',
			'count_lots_bids',
			'histogram_lots_awardDecisionDate']);

		let b = Queries.buildCountrySearchBody(options, country_id);
		b.aggregations = aggs.request;

		//TODO: generalize this hack into a dedicated "add filter to nested aggregations" function
		if (b.query.filtered && b.query.filtered.filter.bool && b.query.filtered.filter.bool.must
			&& b.query.filtered.filter.bool.must[1] && b.query.filtered.filter.bool.must[1].nested) {
			//apply nested filter to aggregations, too
			Queries.applyNestedFilter(b, b.query.filtered.filter.bool.must[1].nested, '');
		}
		if (b.query.filtered && b.query.filtered.filter) {
			let or = b.query.filtered.filter.or || b.query.filtered.filter.bool.must[0].or;
			if (or) {
				let indicator_type = or[0].match_phrase_prefix['indicators.type'];
				b.aggregations['terms_indicators'].terms.include = indicator_type + '.*';
			}
		}

		store.Tender.search(b, 0, 0, (err, result) => {
			if (err) {
				console.log(err);
				return cb(err);
			}
			let stats = aggs.parse(result.aggregations, library, result.hits.total);

			// now get the some aggregations again without indicator filter, so averages can be calculated

			if (b.query.filtered && b.query.filtered.filter.bool && b.query.filtered.filter.bool.must
				&& b.query.filtered.filter.bool.must.length > 1) {
				b.query.filtered.filter.bool.must = [b.query.filtered.filter.bool.must[1]];
			} else if (b.query.filtered) {
				delete b.query.filtered.filter;
			}
			// console.log('query', JSON.stringify(b.query, null, '\t'));
			aggs = Queries.buildAggregations(['histogram_lots_awardDecisionDate', 'terms_main_cpv_divisions']);
			b.aggregations = aggs.request;

			store.Tender.search(b, 0, 0, (err, full_result) => {
				if (err) {
					console.log(err);
					return cb(err);
				}
				let data = aggs.parse(full_result.aggregations, library, full_result.hits.total);

				let histo_pc_per_year = {};
				let stats_histogramm = stats['histogram_lots_awardDecisionDate'];
				let data_histogramm = data['histogram_lots_awardDecisionDate'];
				Object.keys(data_histogramm).forEach(year => {
					let total = (data_histogramm[year] || 0);
					let value = (stats_histogramm[year] || 0);
					if (total > 0 && value > 0) {
						histo_pc_per_year[year] = {
							percent: Math.round((value / (total / 100)) * 100) / 100,
							value: value,
							total: total
						};
					}
				});
				stats.histogram_pc_lots_awardDecisionDate = histo_pc_per_year;

				let terms_pc_main_cpv_divisions = {};
				let stats_terms_main_cpvs = stats['terms_main_cpv_divisions'];
				let data_terms_main_cpvs = data['terms_main_cpv_divisions'];
				Object.keys(stats_terms_main_cpvs).forEach(key => {
					let c = stats_terms_main_cpvs[key];
					let cpc = {
						total: 0,
						percent: 0,
						value: c.value,
						name: c.name
					};
					if (data_terms_main_cpvs[key]) {
						cpc.total = data_terms_main_cpvs[key].value;
						if (cpc.total > 0) {
							cpc.percent = Math.round((cpc.value / (cpc.total / 100)) * 100) / 100;
						}
					}
					terms_pc_main_cpv_divisions[key] = cpc;
				});
				stats.terms_pc_main_cpv_divisions = terms_pc_main_cpv_divisions;

				cb(null, stats);
			});
		});
	};

	this.getCPVStats = (options, country_id, cb) => {
		let id = Utils.validateId(options.id);
		if (!id) return cb(404);
		let cpvInfo = library.parseCPVs(id, options.lang);
		if (!cpvInfo.cpv) {
			return cb(404);
		}
		let b = {};
		b.query = Queries.Filters.byMainCPV(cpvInfo.cpv.id, cpvInfo.cpv.level);
		b.query = Queries.addCountryFilter(b.query, country_id);
		let aggIds = ['top_authorities', 'top_companies', 'sums_finalPrice', 'histogram_lots_awardDecisionDate', 'count_lots_bids'];
		if (cpvInfo.sublevel) {
			aggIds.push('terms_main_cpv_' + cpvInfo.sublevel);
		}
		let aggs = Queries.buildAggregations(aggIds);
		b.aggregations = aggs.request;
		store.Tender.search(b, 0, 0, (err, result) => {
			if (err) {
				return cb(err);
			}
			let stats = aggs.parse(result.aggregations, library, result.hits.total);
			cb(null, {sector: cpvInfo.cpv, parents: cpvInfo.parents, stats: stats});
		});
	};

	this.getRegionStats = (options, country_id, cb) => {
		let id = Utils.validateId(options.id);
		if (!id) return cb(404);
		let nutsInfo = library.parseNUTS(id);
		if (!nutsInfo.nuts || !nutsInfo.nuts.id) {
			return cb(404);
		}
		let b = {};
		b.query = Queries.Filters.byAuthorityNuts(nutsInfo.nuts.id, nutsInfo.nuts.level);
		b.query = Queries.addCountryFilter(b.query, country_id);
		let aggs = Queries.buildAggregations(['top_authorities', 'top_companies', 'sums_finalPrice', 'terms_main_cpv_divisions', 'histogram_lots_awardDecisionDate', 'count_lots_bids']);
		b.aggregations = aggs.request;
		store.Tender.search(b, 0, 0, (err, result) => {
			if (err) {
				return cb(err);
			}
			let stats = aggs.parse(result.aggregations, library, result.hits.total);
			cb(null, {region: nutsInfo.nuts, parents: nutsInfo.parents, stats: stats});
		});

	};

	this.getCompanyStats = (options, country_id, cb) => {
		let ids = Utils.validateIds(options.ids);
		if (!ids) return cb(404);

		let b = {};
		b.query = Queries.Filters.byBidders(ids);
		b.query = Queries.addCountryFilter(b.query, country_id);

		let aggs = Queries.buildAggregations(['terms_main_cpv_divisions']);

		b.aggregations = aggs.request;
		b.aggregations.lotsbids = {
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
										'histogram_lots_awardDecisionDate_reverseNested': Queries.Aggregations.histogram_lots_awardDecisionDate_reverseNested.request,
										'lots_denested': {
											'reverse_nested': {
												'path': 'lots'
											}
										},
										'tender_denested': {
											'reverse_nested': {},
											'aggregations': {
												'terms_authority_nuts': Queries.Aggregations.terms_authority_nuts.request,
												'top_authorities': Queries.Aggregations.top_authorities.request,
												'sums_finalPrice': Queries.Aggregations.sums_finalPrice.request
											}
										}
									}
								}
							}
						}
					}
				},
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
		};

		store.Tender.search(b, 0, 0, (err, result) => {
			if (err) {
				return cb(err);
			}
			let stats = aggs.parse(result.aggregations, library);

			let answer_winning = result.aggregations.lotsbids.lotsbids_nested_filter.lotsbids_nested.lotsbids_nested_filter;

			stats.histogram_lots_awardDecisionDate = Queries.Aggregations.histogram_lots_awardDecisionDate_reverseNested.parse(answer_winning, library);
			stats.sums_finalPrice = Queries.Aggregations.sums_finalPrice.parse(answer_winning.tender_denested, library);
			stats.top_authorities = Queries.Aggregations.top_authorities.parse(answer_winning.tender_denested, library);
			stats.terms_authority_nuts = Queries.Aggregations.terms_authority_nuts.parse(answer_winning.tender_denested, library);

			let answer_all = result.aggregations.lotsbids.lotsbids_nested.lotsbids_nested_filter;
			stats.count_lots_bids = {
				bids: answer_all.doc_count,
				bids_awarded: answer_winning.doc_count,
				lots: answer_all.lots_denested.doc_count,
				tenders: answer_all.tender_denested.doc_count
			};
			cb(null, {stats: stats});
		});
	};

	this.getAuthorityStats = (options, country_id, cb) => {
		let ids = Utils.validateIds(options.ids);
		if (!ids) return cb(404);
		let b = {};
		b.query = Queries.Filters.byBuyers(ids);
		b.query = Queries.addCountryFilter(b.query, country_id);
		let aggs = Queries.buildAggregations(['terms_main_cpv_divisions', 'sums_finalPrice', 'histogram_lots_awardDecisionDate', 'count_lots_bids', 'top_winning_companies', 'terms_company_nuts']);
		b.aggregations = aggs.request;
		store.Tender.search(b, 0, 0, (err, result) => {
			if (err) {
				console.log(err);
				return cb(err);
			}
			let stats = aggs.parse(result.aggregations, library, result.hits.total);
			cb(null, {stats: stats});
		});
	};

	this.getCPVUsageStats = (country_id, cb) => {
		this.getAggregation(country_id, 'terms_main_cpv_divisions', cb);
	};

	this.getCountriesStats = cb => {
		this.getAggregation(null, 'terms_countries', cb);
	};

	this.getCompanyNutsStats = (country_id, cb) => {
		this.getAggregation(country_id, 'terms_company_nuts', cb);
	};

	this.getAuthorityNutsStats = (country_id, cb) => {
		this.getAggregation(country_id, 'terms_authority_nuts', cb);
	};

	this.getHomeStats = (country_id, cb) => {
		this.getAggregations(country_id, ['histogram_lots_awardDecisionDate'], cb);
	};

	this.getMarketAnalysisStats = (options, country_id, cb) => {
		let aggs = Queries.buildAggregations(['terms_main_cpv_divisions']);
		let stats_aggs = Queries.buildAggregations(['top_authorities', 'top_companies', 'sums_finalPrice', 'terms_main_cpv_divisions', 'histogram_lots_awardDecisionDate', 'count_lots_bids']);
		aggs.request['terms_main_cpv_divisions'].aggregations.cpvs_filter.aggregations.divisionscpvs.aggregations = {
			'tender_denested': {
				'reverse_nested': {},
				'aggregations': stats_aggs.request
			}
		};
		store.Tender.aggregations(country_id, aggs.request, (err, data) => {
			if (err) {
				console.log(err);
				return cb(err);
			}
			let result = data.aggregations['terms_main_cpv_divisions'].cpvs_filter.divisionscpvs.buckets.map(bucket => {
				let sector = {
					id: bucket.key,
					name: library.getCPVName(bucket.key, 'EN'),
					value: bucket.doc_count
				};
				return {
					sector: sector,
					stats: stats_aggs.parse(bucket.tender_denested, library, bucket.tender_denested.doc_count)
				};
			});
			cb(null, {sectors_stats: result});
		});
	};

	/**
	 all dbs
	 */

	this.autocomplete = (entity, field, search, country_id, cb) => {
		// console.log('autocomplete', entity, field, search, country_id);
		let index;
		if (entity === 'tender') {
			index = store.Tender;
		} else if (entity === 'company') {
			index = store.Company;
		} else if (entity === 'authority') {
			index = store.Authority;
		} else {
			return cb(null, []);
		}

		let nested = Queries.getNestedField(field);

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

	/**
	 tender dbs
	 */

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

	this.searchTender = (options, country_id, cb) => {
		let body = Queries.buildCountrySearchBody(options, country_id);
		store.Tender.search(body, (options.size || 10), (options.from || 0), (err, result) => {
			if (err) {
				return cb(err);
			}
			if (body.sort) {
				let key = Object.keys(body.sort)[0];
				result.sortBy = {id: key, ascend: body.sort[key].order === 'asc'};
			}
			Queries.compactAggregations(result.aggregations);
			result.hits.hits = result.hits.hits.map(doc => doc._source);
			cb(null, result);
		});
	};

	this.getFieldsUsage = (country_id, cb) => {
		let aggregations = {};
		let ids = {};
		let schema;

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

		let loadSchema = (callback) => {
			fs.readFile(config.data.shared + '/schema.json', (err, data) => {
				if (err) return cb(err);
				schema = JSON.parse(data.toString());
				schema = schemaResolve(schema);
				Object.keys(schema.definitions).forEach(key => {
					schema.definitions[key] = schemaResolve(schema.definitions[key]);
				});
				callback(schema);
			});
		};

		let scanSchema = (p, obj) => {
			if (!obj.properties) return;
			Object.keys(obj.properties).forEach(key => {
				let field = p.concat([key]).join('.');
				let nested = Queries.getNestedField(field);
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
				if (prop.type === 'object') {
					scanSchema(p.concat([key]), prop);
				} else if (prop.type === 'array') {
					scanSchema(p.concat([key]), prop.items);
				}
			});
		};

		loadSchema((schema) => {
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
						if (value === 'distinct') {
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
		});

	};

	this.getTopExpensiveTenders = (country_id, cb) => {
		let b = {
			query: {
				'match_all': {}
			},
			sort: {
				'finalPrice.netAmount': {
					'order': 'desc'
				}
			}
		};
		if (country_id) {
			b.query = {
				term: {
					'country': country_id
				}
			};
		}
		store.Tender.search(b, 10, 0, (err, result) => {
			if (err) {
				return cb(err);
			}
			result = result.hits.hits.map(t => {
				t = t._source;
				return {
					id: t.id,
					finalPrice: t.finalPrice
				};
			});
			cb(null, result);
		});
	};

	/**
	 company & authority dbs
	 */

	this.searchAuthority = (options, country_id, cb) => {

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

		let body = Queries.buildCountrySearchBody(options, country_id);
		searchAuthorities(body, (options.size || 10), (options.from || 0), cb);
	};

	this.searchCompany = (options, country_id, cb) => {

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

		let body = Queries.buildCountrySearchBody(options, country_id);
		searchCompanies(body, (options.size || 10), (options.from || 0), cb);


	};

	this.searchSimilarCompany = (id, country_id, cb) => {

		let searchSimilarCompanyName = (name, ignoreIds, country_id, cb) => {
			let body = {
				'query': {
					'bool': {
						'must': [
							{
								'match_phrase': {
									'body.name.slug': name || ''
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

		if ((typeof id !== 'string') || ( id.trim() === '')) return cb(404);
		store.Company.get(id, (err, result) => {
			if (err) return cb(err);
			if (result.hits.hits.length === 0) return cb(404);
			let company = result.hits.hits[0]._source;
			searchSimilarCompanyName(company.body.name, [id], country_id, cb);
		});
	};

	this.searchSimilarAuthority = (id, country_id, cb) => {

		let searchSimilarAuthorityName = (name, ignoreIds, country_id, cb) => {
			let body = {
				'query': {
					'bool': {
						'must': [
							{
								'match_phrase': {
									'body.name.slug': name || ''
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

		if ((typeof id !== 'string') || ( id.trim() === '')) return cb(404);
		store.Authority.get(id, (err, result) => {
			if (err) return cb(err);
			if (result.hits.hits.length === 0) return cb(404);
			let company = result.hits.hits[0]._source;
			searchSimilarAuthorityName(company.body.name, [id], country_id, cb);
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

	this.getAuthority = (id, country_id, cb) => {
		if ((typeof id !== 'string') || (id.trim() === '')) return cb(404);
		store.Authority.get(id, (err, result) => {
			if (err) return cb(err);
			if (result.hits.hits.length === 0) return cb(404);
			cb(null, {authority: result.hits.hits[0]._source});
		});
	};

	/**
	 init
	 */

	this.init = cb => {
		store.init(err => {
			// this.getAggregations('DE', ['companies_value'], function (err, stats) {
			// 	console.log(err, stats);
			// });
			// this.getTopExpensiveTenders('DE', () => {
			// });
			// this.getAuthorityNutsStats('DE', (err, stats) => {
			// 	console.log(JSON.stringify(stats, null, '\t'));
			// });
			cb(err);
		});
	};

};

module.exports = Api;