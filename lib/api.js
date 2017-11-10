const fs = require('fs');
const zlib = require('zlib');
const Store = require('./store.js');
const Library = require('./library');
const Queries = require('./queries');
const Utils = require('./utils');

class Api {

	constructor(config) {
		this.library = new Library(config);
		this.store = new Store(config);
		this.config = config;
	}

	/**
	 tender dbs aggregations
	 */

	getAggregations(country_id, aggregation_ids, cb) {
		let aggs = Queries.buildAggregations(aggregation_ids);
		this.store.Tender.aggregations(country_id, aggs.request, (err, result) => {
			if (err) {
				console.log(err);
				return cb(err);
			}
			let stats = aggs.parse(result.aggregations, {library: this.library, tendercount: result.hits.total});
			cb(null, stats);
		});
	}

	getAggregation(country_id, aggregation_id, cb) {
		this.getAggregations(country_id, [aggregation_id], (err, stats) => {
			if (err) {
				return cb(err);
			}
			cb(null, stats[aggregation_id]);
		});
	}

	getIndicatorStats(options, country_id, cb) {

		let indicatorFilter = (options.filters || []).filter(f => f.field === 'scores.type' || f.field === 'indicators.type')[0];
		let indicator = indicatorFilter && indicatorFilter.value ? indicatorFilter.value[0] : '';
		let isSubindicator = indicatorFilter && indicatorFilter.field === 'indicators.type';

		let aggs = Queries.buildAggregations([
			'terms_main_cpv_divisions',
			'top_terms_companies',
			'top_terms_authorities',
			'top_sum_finalPrice_companies',
			'top_sum_finalPrice_authorities',
			'histogram_lots_awardDecisionDate_finalPriceEUR'
		]);

		let b = Queries.buildCountrySearchBody(options, country_id);

		b.aggregations = aggs.request;

		if (b.query.bool && b.query.bool.filter) {
			Queries.applyNestedFilterToAggregations(b.aggregations, b.query.bool.filter);
		}
		// console.log('a', JSON.stringify(b, null, '\t'));

		this.store.Tender.search(b, 0, 0, (err, result) => {
			// console.log('b', JSON.stringify(result, null, '\t'));
			if (err) {
				console.log(err);
				return cb(err);
			}
			let stats = aggs.parse(result.aggregations, {library: this.library, tendercount: result.hits.total, lang: options.lang});

			// now get the some aggregations again without indicator filter, so averages can be calculated
			if (b.query.bool && b.query.bool.filter) {
				b.query.bool.filter = b.query.bool.filter.filter(filter => {
					return !(filter.nested && ['indicators', 'scores'].indexOf(filter.nested.path) >= 0);
				});
				if (b.query.bool.filter.length === 0) {
					b.query = {match_all: {}};
				}
			}

			aggs = Queries.buildAggregations([
				'histogram_lots_awardDecisionDate',
				'terms_main_cpv_divisions',
				isSubindicator ? 'terms_main_cpv_divisions_indicators' : 'terms_main_cpv_divisions_scores',
				isSubindicator ? 'histogram_lots_awardDecisionDate_indicators' : 'histogram_lots_awardDecisionDate_scores',
				'terms_indicators_score']);
			b.aggregations = aggs.request;

			this.store.Tender.search(b, 0, 0, (err, full_result) => {
				if (err) {
					console.log(err);
					return cb(err);
				}
				let data = aggs.parse(full_result.aggregations, {library: this.library, tendercount: full_result.hits.total, lang: options.lang});
				let histo_pc_per_year = {};
				let stats_histogram = stats['histogram_lots_awardDecisionDate_finalPriceEUR'];
				let data_histogram = data['histogram_lots_awardDecisionDate'];
				Object.keys(data_histogram).forEach(year => {
					let total = (data_histogram[year] || 0);
					let value = (stats_histogram[year] ? stats_histogram[year].value : 0);
					if (total > 0 && value > 0) {
						histo_pc_per_year[year] = {
							percent: value / (total / 100),
							value: value,
							total: total,
							sum_finalPriceEUR: stats_histogram[year].sum_finalPriceEUR,
							avg_finalPriceEUR: stats_histogram[year].avg_finalPriceEUR
						};
					}
				});


				//TODO: make one histogram request with the needed score instead of get all and filter
				stats.histogram_lots_awardDecisionDate_avg_scores = {};
				if (data.histogram_lots_awardDecisionDate_indicators) {
					Object.keys(data.histogram_lots_awardDecisionDate_indicators).forEach(key => {
						if (key.indexOf(indicator) === 0) {
							stats.histogram_lots_awardDecisionDate_avg_scores[key] = data.histogram_lots_awardDecisionDate_indicators[key];
						}
					});
				}
				if (data.histogram_lots_awardDecisionDate_scores) {
					Object.keys(data.histogram_lots_awardDecisionDate_scores).forEach(key => {
						if (key.indexOf(indicator) === 0) {
							stats.histogram_lots_awardDecisionDate_avg_scores[key] = data.histogram_lots_awardDecisionDate_scores[key];
						}
					});
				}

				//TODO: make one avg_terms request with the needed score instead of get all and filter
				stats.terms_main_cpv_divisions_avg_scores = {};
				if (data.terms_main_cpv_divisions_scores) {
					Object.keys(data.terms_main_cpv_divisions_scores).forEach(key => {
						let part = data.terms_main_cpv_divisions_scores[key];
						if (part.scores[indicator] !== null) {
							stats.terms_main_cpv_divisions_avg_scores[key] = {
								name: part.name,
								value: part.scores[indicator]
							};
						}
					});
				}
				if (data.terms_main_cpv_divisions_indicators) {
					Object.keys(data.terms_main_cpv_divisions_indicators).forEach(key => {
						let part = data.terms_main_cpv_divisions_indicators[key];
						if (part.scores[indicator] !== null) {
							stats.terms_main_cpv_divisions_avg_scores[key] = {
								name: part.name,
								value: part.scores[indicator]
							};
						}
					});
				}

				stats.terms_indicators_score = {};
				Object.keys(data.terms_indicators_score).forEach(key => {
					if (key.indexOf(indicator) === 0) {
						stats.terms_indicators_score[key] = data.terms_indicators_score[key];
					}
				});
				stats.histogram_lots_awardDecisionDate_finalPriceEUR = undefined;
				stats.histogram_pc_lots_awardDecisionDate_finalPrices = histo_pc_per_year;

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

				this.fillAggregationBodies(stats, (err) => {
					if (err) {
						return cb(err);
					}
					cb(null, stats);
				});

			});
		});
	}

	getMarketAnalysisStats(options, country_id, cb) {
		let aggs = Queries.buildAggregations(['terms_main_cpv_divisions', 'histogram_lots_awardDecisionDate_scores',
			'terms_main_cpv_divisions_scores', 'histogram_lots_awardDecisionDate_finalPriceEUR',
			'terms_procedure_type',
			'top_terms_companies',
			'top_terms_authorities',
			'top_sum_finalPrice_companies',
			'top_sum_finalPrice_authorities',
			'terms_authority_nuts', 'histogram_lots_awardDecisionDate']);
		let cpv_stats_aggs = Queries.buildAggregations(['sum_finalPriceEUR']);
		let nuts_stats_aggs = Queries.buildAggregations(['sum_finalPriceEUR', 'avg_finalPriceEUR']);
		aggs.request['terms_main_cpv_divisions'].aggregations.cpvs_filter.aggregations.cpvs.aggregations = {
			'tender_denested': {
				'reverse_nested': {},
				'aggregations': cpv_stats_aggs.request
			}
		};
		aggs.request['terms_authority_nuts'].aggregations.authority_nuts_nested.aggregations = {
			'tender_denested': {
				'reverse_nested': {},
				'aggregations': nuts_stats_aggs.request
			}
		};
		let b = Queries.buildCountrySearchBody(options, country_id);
		b.aggregations = aggs.request;

		if (b.query.bool && b.query.bool.filter) {
			Queries.applyNestedFilterToAggregations(b.aggregations, b.query.bool.filter);
		}
		this.store.Tender.search(b, 0, 0, (err, data) => {
			if (err) {
				console.log(err);
				return cb(err);
			}

			let sectors_stats = data.aggregations['terms_main_cpv_divisions'].cpvs_filter.cpvs.buckets.map(bucket => {
				let sector = {
					id: bucket.key,
					name: this.library.getCPVName(bucket.key, options.lang),
					value: bucket.doc_count
				};
				return {
					sector: sector,
					stats: cpv_stats_aggs.parse(bucket.tender_denested, {library: this.library, tendercount: bucket.tender_denested.doc_count, lang: options.lang})
				};
			});
			let region_stats = data.aggregations['terms_authority_nuts'].authority_nuts_nested.buckets.map(bucket => {
				return {
					id: bucket.key,
					value: bucket.doc_count,
					stats: nuts_stats_aggs.parse(bucket.tender_denested, {library: this.library, tendercount: bucket.tender_denested.doc_count, lang: options.lang})
				};
			});
			let stats = aggs.parse(data.aggregations, {library: this.library, lang: options.lang});

			stats.terms_main_cpv_divisions_avg_scores = {};
			if (stats.terms_main_cpv_divisions_scores) {
				Object.keys(stats.terms_main_cpv_divisions_scores).forEach(key => {
					let part = stats.terms_main_cpv_divisions_scores[key];
					if (part.scores['TENDER'] !== null) {
						stats.terms_main_cpv_divisions_avg_scores[key] = {
							name: part.name,
							value: part.scores['TENDER']
						};
					}
				});
			}
			let histo_per_year = {};
			let stats_histogram = stats['histogram_lots_awardDecisionDate_finalPriceEUR'];
			Object.keys(stats_histogram).forEach(year => {
				let value = (stats_histogram[year] ? stats_histogram[year].value : 0);
				if (value > 0) {
					histo_per_year[year] = {
						value: value,
						sum_finalPriceEUR: stats_histogram[year].sum_finalPriceEUR,
						avg_finalPriceEUR: stats_histogram[year].avg_finalPriceEUR
					};
				}
			});
			stats.histogram_lots_awardDecisionDate_finalPrices = histo_per_year;
			stats.histogram_lots_awardDecisionDate_finalPriceEUR = undefined;
			stats.histogram_lots_awardDecisionDate_avg_scores = stats.histogram_lots_awardDecisionDate_scores;
			stats.terms_main_cpv_divisions_scores =  undefined;
			stats.histogram_lots_awardDecisionDate_scores = undefined;
			stats.terms_authority_nuts = undefined;
			stats.terms_main_cpv_divisions = undefined;
			stats.sectors_stats = sectors_stats;
			stats.region_stats = region_stats;

			this.fillAggregationBodies(stats, (err) => {
				if (err) {
					return cb(err);
				}
				cb(null, stats);
			});
		});
	}

	getSectorStats(options, country_id, cb) {
		let ids = Utils.validateIds(options.ids);
		if (!ids) {
			return cb(404);
		}
		let id = Utils.validateId(ids[0]);
		if (!id) {
			return cb(404);
		}
		let cpvInfo = this.library.parseCPVs(id, options.lang);
		if (!cpvInfo.cpv) {
			return cb(404);
		}
		let b = Queries.buildCountrySearchBody(options, country_id);
		b.query.bool.filter.push(Queries.Filters.byMainCPV(cpvInfo.cpv.id, cpvInfo.cpv.level));

		let aggIds = [
			'top_terms_companies',
			'top_terms_authorities',
			'top_sum_finalPrice_companies',
			'top_sum_finalPrice_authorities',
			'histogram_lots_awardDecisionDate_finalPriceEUR',
			'terms_procedure_type',
			'terms_authority_nuts'
		];
		let subaggregationId = null;
		if (cpvInfo.sublevel) {
			subaggregationId = 'terms_main_cpv_' + cpvInfo.sublevel;
			aggIds.push(subaggregationId);
		}
		let aggs = Queries.buildAggregations(aggIds);
		let stats_aggs = Queries.buildAggregations(['sum_finalPriceEUR']);

		if (subaggregationId) {
			aggs.request[subaggregationId].aggregations.cpvs_filter.aggregations.cpvs.aggregations = {
				'tender_denested': {
					'reverse_nested': {},
					'aggregations': stats_aggs.request
				}
			};
		}

		b.aggregations = aggs.request;

		if (b.query.bool && b.query.bool.filter) {
			Queries.applyNestedFilterToAggregations(b.aggregations, b.query.bool.filter);
		}
		this.store.Tender.search(b, 0, 0, (err, data) => {
			if (err) {
				return cb(err);
			}
			let stats = aggs.parse(data.aggregations, {library: this.library, tendercount: data.hits.total, lang: options.lang});

			if (subaggregationId) {
				let answer = Queries.getAnswerAggreggation(data.aggregations, subaggregationId);
				let result = answer.cpvs_filter.cpvs.buckets.map(bucket => {
					let sector = {
						id: bucket.key,
						name: this.library.getCPVName(bucket.key, options.lang),
						value: bucket.doc_count
					};
					return {
						sector: sector,
						stats: stats_aggs.parse(bucket.tender_denested, {library: this.library, tendercount: bucket.tender_denested.doc_count, lang: options.lang})
					};
				});
				stats[subaggregationId] = undefined;
				stats.sectors_stats = result;
			}

			// now get the some aggregations again without cpv filter, so averages can be calculated
			if (b.query.bool && b.query.bool.filter) {
				b.query.bool.filter = b.query.bool.filter.filter(filter => {
					return (!filter.nested) || (filter.nested.path !== 'cpvs');
				});
				if (b.query.bool.filter.length === 0) {
					b.query = {match_all: {}};
				}
			}

			aggs = Queries.buildAggregations(['histogram_lots_awardDecisionDate']);
			b.aggregations = aggs.request;

			this.store.Tender.search(b, 0, 0, (err, full_result) => {
				if (err) {
					console.log(err);
					return cb(err);
				}
				let full_data = aggs.parse(full_result.aggregations, {library: this.library, tendercount: full_result.hits.total, lang: options.lang});

				let histo_pc_per_year = {};
				let stats_histogram = stats['histogram_lots_awardDecisionDate_finalPriceEUR'];
				let data_histogram = full_data['histogram_lots_awardDecisionDate'];
				Object.keys(data_histogram).forEach(year => {
					let total = (data_histogram[year] || 0);
					let value = (stats_histogram[year] ? stats_histogram[year].value : 0);
					if (total > 0 && value > 0) {
						histo_pc_per_year[year] = {
							percent: Math.round((value / (total / 100)) * 100) / 100,
							value: value,
							total: total,
							sum_finalPriceEUR: stats_histogram[year].sum_finalPriceEUR,
							avg_finalPriceEUR: stats_histogram[year].avg_finalPriceEUR
						};
					}
				});
				stats.histogram_lots_awardDecisionDate_finalPriceEUR = undefined;
				stats.histogram_pc_lots_awardDecisionDate_finalPrices = histo_pc_per_year;


				this.fillAggregationBodies(stats, (err) => {
					if (err) {
						return cb(err);
					}
					cb(null, {sector: cpvInfo.cpv, parents: cpvInfo.parents, stats: stats});
				});

			});
		});
	}

	getRegionStats(options, country_id, cb) {
		let ids = Utils.validateIds(options.ids);
		if (!ids) {
			return cb(404);
		}
		let id = Utils.validateId(ids[0]);
		if (!id) {
			return cb(404);
		}
		let nutsInfo = this.library.parseNUTS(id);
		if (!nutsInfo.nuts || !nutsInfo.nuts.id) {
			return cb(404);
		}
		let b = {};
		b.query = Queries.Filters.byAuthorityNuts(nutsInfo.nuts.id, nutsInfo.nuts.level);
		b.query = Queries.addCountryFilter(b.query, country_id);
		let aggs = Queries.buildAggregations([
			'top_terms_companies',
			'top_terms_authorities',
			'top_sum_finalPrice_companies',
			'top_sum_finalPrice_authorities',
			'terms_main_cpv_divisions',
			'histogram_lots_awardDecisionDate']);
		b.aggregations = aggs.request;
		this.store.Tender.search(b, 0, 0, (err, result) => {
			if (err) {
				return cb(err);
			}
			let stats = aggs.parse(result.aggregations, {library: this.library, tendercount: result.hits.total, lang: options.lang});
			this.fillAggregationBodies(stats, (err) => {
				if (err) {
					return cb(err);
				}
				cb(null, {region: nutsInfo.nuts, parents: nutsInfo.parents, stats: stats});
			});
		});
	}

	getCompanyStats(options, country_id, cb) {
		let ids = Utils.validateIds(options.ids);
		if (!ids) {
			return cb(404);
		}

		let b = {};
		b.query = Queries.Filters.byBidders(ids);
		b.query = Queries.addCountryFilter(b.query, country_id);

		let aggs = Queries.buildAggregations(['terms_main_cpv_divisions']);
		let subaggs = Queries.buildAggregations(['terms_authority_nuts', 'top_terms_authorities', 'top_sum_finalPrice_authorities']);
		b.aggregations = aggs.request;
		b.aggregations.lotsbids = {
			'nested': {
				'path': 'lots.bids'
			},
			'aggregations': {
				'lotsbids_winning_nested_filter': {
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
											'lots.bids.bidders.id': ids
										}
									},
									'aggregations': {
										'histogram_lots_awardDecisionDate_finalPriceEUR_reverseNested': {
											'reverse_nested': {
												'path': 'lots'
											},
											'aggregations': {
												'dates_nested': {
													'date_histogram': {'field': 'lots.awardDecisionDate', 'interval': 'year'},
													'aggregations': {
														'tender_denested': {
															'reverse_nested': {},
															'aggregations': {
																'lot_count': {'filter': Queries.Filters.byBidders(ids)},
																'sum_finalPriceEUR': {
																	'sum': {
																		'field': 'finalPrice.netAmountEur'
																	}
																},
																'avg_finalPriceEUR': {
																	'avg': {
																		'field': 'finalPrice.netAmountEur'
																	}
																}
															}
														}
													}
												}
											}
										},
										'tender_denested': {
											'reverse_nested': {},
											'aggregations': subaggs.request
										}
									}
								}
							}
						}
					}
				}
			}
		};

		let parseoptions = {library: this.library, lang: options.lang};

		this.store.Tender.search(b, 0, 0, (err, result) => {
			if (err) {
				return cb(err);
			}
			let stats = aggs.parse(result.aggregations, parseoptions);
			let answer_winning = subaggs.parse(result.aggregations.lotsbids.lotsbids_winning_nested_filter.lotsbids_nested.lotsbids_nested_filter.tender_denested, parseoptions);
			Object.keys(answer_winning).forEach(key => {
				stats[key] = answer_winning[key];
			});
			let stats_histogram = result.aggregations.lotsbids.lotsbids_winning_nested_filter.lotsbids_nested.lotsbids_nested_filter.histogram_lots_awardDecisionDate_finalPriceEUR_reverseNested.dates_nested;
			let histo_per_year = {};
			stats_histogram.buckets.forEach(bucket => {
				let year = parseInt(bucket.key_as_string.slice(0, 4), 10);
				if (Utils.isValidDigiwhistYear(year)) {
					let sub_buckets = bucket.tender_denested;
					histo_per_year[year] = {value: sub_buckets.lot_count.doc_count, sum_finalPriceEUR: sub_buckets.sum_finalPriceEUR, avg_finalPriceEUR: sub_buckets.avg_finalPriceEUR};
				}
			});
			stats.histogram_lots_awardDecisionDate_finalPrices = histo_per_year;

			this.fillAggregationBodies(stats, (err) => {
				if (err) {
					return cb(err);
				}
				cb(null, {stats: stats});
			});
		});
	}

	getAuthorityStats(options, country_id, cb) {
		let ids = Utils.validateIds(options.ids);
		if (!ids) {
			return cb(404);
		}
		let b = {};
		b.query = Queries.Filters.byBuyers(ids);
		b.query = Queries.addCountryFilter(b.query, country_id);
		let aggs = Queries.buildAggregations([
			'terms_main_cpv_divisions',
			'top_terms_companies',
			'top_sum_finalPrice_companies',
			'terms_company_nuts',
			'histogram_lots_awardDecisionDate_finalPriceEUR',
			'terms_indicators_score'
		]);
		b.aggregations = aggs.request;
		this.store.Tender.search(b, 0, 0, (err, result) => {
			if (err) {
				console.log(err);
				return cb(err);
			}
			let stats = aggs.parse(result.aggregations, {library: this.library, lang: options.lang});

			let histo_per_year = {};
			let stats_histogram = stats['histogram_lots_awardDecisionDate_finalPriceEUR'];
			Object.keys(stats_histogram).forEach(year => {
				let value = (stats_histogram[year] ? stats_histogram[year].value : 0);
				if (value > 0) {
					histo_per_year[year] = {
						value: value,
						sum_finalPriceEUR: stats_histogram[year].sum_finalPriceEUR,
						avg_finalPriceEUR: stats_histogram[year].avg_finalPriceEUR
					};
				}
			});
			stats.histogram_lots_awardDecisionDate_finalPriceEUR = undefined;
			stats.histogram_lots_awardDecisionDate_finalPrices = histo_per_year;

			this.fillAggregationBodies(stats, (err) => {
				if (err) {
					return cb(err);
				}
				cb(null, {stats: stats});
			});
		});
	}

	getCPVUsageStats(country_id, cb) {
		this.getAggregation(country_id, 'terms_main_cpv_divisions', cb);
	}

	getCountriesStats(cb) {
		this.getAggregation(null, 'terms_countries', cb);
	}

	getCompanyNutsStats(country_id, cb) {
		this.getAggregation(country_id, 'terms_company_nuts', cb);
	}

	getAuthorityNutsStats(country_id, cb) {
		this.getAggregation(country_id, 'terms_authority_nuts', cb);
	}

	getHomeStats(country_id, cb) {
		this.getAggregations(country_id, ['histogram_lots_awardDecisionDate'], cb);
	}

	/**
	 all dbs
	 */

	autocomplete(entity, field, search, country_id, cb) {
		// console.log('autocomplete', entity, field, search, country_id);
		let index;
		let country_term;
		if (entity === 'tender') {
			index = this.store.Tender;
			country_term = {'country': country_id};
		} else if (entity === 'company') {
			index = this.store.Supplier;
			country_term = {'sources.country': country_id};
		} else if (entity === 'authority') {
			index = this.store.Buyer;
			country_term = {'sources.country': country_id};
		} else {
			return cb(null, []);
		}
		if ((search || '').trim().length === 0) {
			return cb(null, []);
		}

		let nested = Utils.getNestedField(field);

		let query_field = null;
		let query_field_pure = null;
		if (search && search.length > 0) {
			query_field_pure = {
				match_phrase_prefix: {}
			};
			query_field_pure.match_phrase_prefix[field] = search;
			if (nested) {
				query_field = {
					'nested': {
						'path': nested,
						query: query_field_pure
					}
				};
			} else {
				query_field = query_field_pure;
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
										'filter': [
											query_field_pure
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
				bool: {
					filter: [
						{term: country_term}
					]
				}
			},
			aggregations: aggfield
		};
		if (query_field) {
			body.query.bool.filter.push(query_field);
		}
		index.search(body, 0, 0, (err, result) => {
			if (err) {
				return cb(err);
			}
			result = result.aggregations.nestedresult ? result.aggregations.nestedresult['nested-filter'].result : result.aggregations.result;
			cb(null, result.buckets.map((item) => {
				return {key: item.key, value: item.doc_count};
			}));
		});

	}

	/**
	 tender dbs
	 */

	getTender(options, cb) {
		let id = Utils.validateId(options.id);
		if (!id) {
			return cb(404);
		}
		this.store.Tender.get(id, (err, result) => {
			if (err || !result) {
				cb(404);
			} else {
				if (result.hits.total > 1) {
					console.log('warning', 'multiple tender found for one id', id);
				}
				if (result.hits.total < 1) {
					cb(404);
				} else {
					let tender = result.hits.hits[0]._source;
					this.fillTenderData(tender, options.lang);
					cb(null, tender);
				}
			}
		});
	}

	streamSearch(size, stream, options, country_id, canContinueFN) {
		let query = Queries.buildCountrySearchBody(options, country_id);
		let first = true;
		stream.write('[');
		this.store.Tender.streamQuery(size, query.query,
			(items) => {
				if (items.length > 0) {
					if (!first) {
						stream.write(',');
					} else {
						first = false;
					}
					stream.write(items.map(item => {
						return JSON.stringify(item._source);
					}).join(','));
				}
				return !(canContinueFN && !canContinueFN());

			},
			(err) => {
				if (!err) {
					stream.write(']');
				}
				stream.end();
			});
	}

	streamTender(id, req, res, options, country_id) {
		res.setHeader('Content-Type', 'application/gzip');
		res.setHeader('Cache-Control', 'no-cache');
		res.setHeader('Content-disposition', 'attachment; filename=' + id + '.tenders.json.gz');

		const stream = zlib.createGzip();

		stream.pipe(res);

		let closed = false;

		req.connection.on('close', () => {
			// console.log('download connection close');
			closed = true;
		});

		stream.on('close', () => {
			res.end();
			closed = true;
		});

		this.streamSearch(100, stream, options, country_id, () => {
			return !closed;
		});
	}

	searchTender(options, country_id, cb) {
		let body = Queries.buildCountrySearchBody(options, country_id);
		this.store.Tender.search(body, (options.size || 10), (options.from || 0), (err, result) => {
			if (err) {
				return cb(err);
			}
			Queries.parseSearchAggregations(result.aggregations, {library: this.library, lang: options.lang});

			let answer = {
				hits: {
					total: result.hits.total,
					hits: result.hits.hits.map(doc => {
						this.fillTenderData(doc._source, options.lang);
						return doc._source;
					})
				},
				aggregations: result.aggregations
			};
			if (body.sort) {
				let key = Object.keys(body.sort)[0];
				answer.sortBy = {id: key, ascend: body.sort[key].order === 'asc'};
			}
			cb(null, answer);
		});
	}

	fillTenderData(tender, lang) {
		if (tender.cpvs) {
			tender.cpvs.forEach(cpv => {
				cpv.name = this.library.getCPVName(cpv.code, lang);
			});
		}
	}

	getFieldsUsage(country_id, cb) {
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
			if (obj.items) {
				obj.items = schemaResolve(obj.items);
			}
			if (obj.properties) {
				Object.keys(obj.properties).forEach(key => {
					obj.properties[key] = schemaResolve(obj.properties[key]);
				});
			}
			return obj;
		};

		let loadSchema = (callback) => {
			fs.readFile(this.config.data.shared + '/schema.json', (err, data) => {
				if (err) {
					return cb(err);
				}
				schema = JSON.parse(data.toString());
				// schema = schemaResolve(schema);
				Object.keys(schema.definitions).forEach(key => {
					schema.definitions[key] = schemaResolve(schema.definitions[key]);
				});
				callback(schema);
			});
		};

		let scanSchema = (p, obj) => {
			if (!obj.properties) {
				return;
			}
			Object.keys(obj.properties).forEach(key => {
				let field = p.concat([key]).join('.');
				let nested = Utils.getNestedField(field);
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
				if (prop['distinct']) {
					aggs[id + '#distinct'] = {'cardinality': {'field': field}};
				}
				if (prop.type === 'object') {
					scanSchema(p.concat([key]), prop);
				} else if (prop.type === 'array') {
					scanSchema(p.concat([key]), prop.items);
				}
			});
		};

		loadSchema(() => {
			scanSchema([], schema.definitions.tender);

			this.store.Tender.aggregations(country_id, aggregations, (err, response) => {
				if (err) {
					return cb(err);
				}
				let results = {};

				let resolveResult = (key, result) => {
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
						if (a.field < b.field) {
							return -1;
						}
						if (a.field > b.field) {
							return 1;
						}
						return 0;
					})
				);
			});
		});

	}

	getTopExpensiveTenders(country_id, cb) {
		let b = {
			query: {
				'match_all': {}
			},
			sort: {
				'finalPrice.netAmountEur': {
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
		this.store.Tender.search(b, 10, 0, (err, result) => {
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
		})
	}

	getCPV(options, cb) {
		let id = Utils.validateId(options.id);
		if (!id) {
			return cb(404);
		}
		let cpvInfo = this.library.parseCPVs(id, options.lang);
		if (!cpvInfo.cpv) {
			return cb(404);
		}
		cb(null, {sector: cpvInfo.cpv, parents: cpvInfo.parents});
	}

	/**
	 company & authority dbs
	 */

	searchBuyer(options, country_id, cb) {
		let body = Queries.buildSearchBody(options, true);
		if (country_id) {
			body = Queries.addCountrySearchBody(body, {'sources.country': country_id});
		}
		this.store.Buyer.search(body, options.size || 10, options.from || 0, (err, result) => {
			if (err) {
				return cb(err);
			}
			Queries.parseSearchAggregations(result.aggregations, {library: this.library, lang: options.lang});
			let answer = {
				hits: {
					total: result.hits.total,
					hits: result.hits.hits.map(hit => hit._source)
				},
				aggregations: result.aggregations
			};
			if (body.sort) {
				let key = Object.keys(body.sort)[0];
				answer.sortBy = {id: key, ascend: body.sort[key].order === 'asc'};
			}
			cb(null, answer);
		});
	}

	searchSupplier(options, country_id, cb) {
		let body = Queries.buildSearchBody(options, true);
		if (country_id) {
			body = Queries.addCountrySearchBody(body, {'sources.country': country_id});
		}
		this.store.Supplier.search(body, options.size || 10, options.from || 0, (err, result) => {
			if (err) {
				return cb(err);
			}
			Queries.parseSearchAggregations(result.aggregations, {library: this.library, lang: options.lang});
			let answer = {
				hits: {
					total: result.hits.total,
					hits: result.hits.hits.map(hit => hit._source)
				},
				aggregations: result.aggregations
			};
			if (body.sort) {
				let key = Object.keys(body.sort)[0];
				answer.sortBy = {id: key, ascend: body.sort[key].order === 'asc'};
			}
			cb(null, answer);
		});
	}

	fillAggregationBodies(stats, cb) {
		this.fillAggregationAuthorityBodies(stats, (err) => {
			if (err) {
				return cb(err);
			}
			this.fillAggregationCompanyBodies(stats, cb);
		});
	}

	fillAggregationAuthorityBodies(stats, cb) {
		let ids = [];
		if (stats.top_sum_finalPrice_authorities) {
			ids = stats.top_sum_finalPrice_authorities.top10.map(entry => {
				return entry.id;
			});
		}
		if (stats.top_terms_authorities) {
			ids = ids.concat(stats.top_terms_authorities.top10.map(entry => {
				return entry.id;
			}));
		}
		if (ids.length === 0) {
			return cb();
		}
		this.store.Buyer.getByIds(ids, (err, result) => {
			if (err) {
				return cb(err);
			}
			let results = {};
			result.hits.hits.forEach(hit => {
				results[hit._source.body.id] = hit._source.body;
			});

			if (stats.top_sum_finalPrice_authorities) {
				stats.top_sum_finalPrice_authorities.top10.forEach(entry => {
					entry.body = results[entry.id] || {};
				});
			}
			if (stats.top_terms_authorities) {
				stats.top_terms_authorities.top10.forEach(entry => {
					entry.body = results[entry.id] || {};
				});
			}
			cb();
		});
	}

	fillAggregationCompanyBodies(stats, cb) {
		let ids = [];
		if (stats.top_sum_finalPrice_companies) {
			ids = stats.top_sum_finalPrice_companies.top10.map(entry => {
				return entry.id;
			});
		}
		if (stats.top_terms_companies) {
			ids = ids.concat(stats.top_terms_companies.top10.map(entry => {
				return entry.id;
			}));
		}
		if (ids.length === 0) {
			return cb();
		}
		this.store.Supplier.getByIds(ids, (err, result) => {
			if (err) {
				return cb(err);
			}
			let results = {};
			result.hits.hits.forEach(hit => {
				results[hit._source.body.id] = hit._source.body;
			});

			if (stats.top_sum_finalPrice_companies) {
				stats.top_sum_finalPrice_companies.top10.forEach(entry => {
					entry.body = results[entry.id] || {};
					if (!entry.body.id) {
						console.log('alarm', 'body not found', entry);
					}
					entry.id = undefined;
				});
			}
			if (stats.top_terms_companies) {
				stats.top_terms_companies.top10.forEach(entry => {
					entry.body = results[entry.id] || {};
					if (!entry.body.id) {
						console.log('alarm', 'body not found', entry);
					}
					entry.id = undefined;
				});
			}
			// console.log(JSON.stringify(result, null, '\t'));
			cb();
		});
	}

	searchSimilarCompany(id, country_id, cb) {

		let searchSimilarCompanyName = (name, ignoreIds) => {
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
				}
			};
			if (ignoreIds && ignoreIds.length > 0) {
				body.query.bool.must_not = {
					terms: {'body.id': ignoreIds}
				};
			}
			if (country_id) {
				body.query.bool.must.push({'term': {'sources.country': country_id}});
			}
			this.store.Supplier.search(body, 100, 0, (err, results) => {
				if (err) {
					return cb(err);
				}
				let similar = results.hits.hits.map(hit => {
					return {value: hit._source.sources.length, body: hit._source.body, country: hit._source.body && hit._source.body.address ? hit._source.body.address.country : null};
				});
				cb(null, {similar: similar});
			});
		};

		id = Utils.validateId(id);
		if (!id) {
			return cb(404);
		}
		this.store.Supplier.get(id, (err, result) => {
			if (err) {
				return cb(err);
			}
			if (result.hits.hits.length === 0) {
				return cb(404);
			}
			let company = result.hits.hits[0]._source;
			searchSimilarCompanyName(company.body.name, [id]);
		});
	}

	searchSimilarAuthority(id, country_id, cb) {

		let searchSimilarAuthorityName = (name, ignoreIds) => {
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
				}
			};

			if (ignoreIds && ignoreIds.length > 0) {
				body.query.bool.must_not = {
					terms: {'body.id': ignoreIds}
				};
			}

			if (country_id) {
				body.query.bool.must.push({'term': {'sources.country': country_id}});
			}
			this.store.Buyer.search(body, 100, 0, (err, results) => {
				if (err) {
					return cb(err);
				}
				let similar = results.hits.hits.map(hit => {
					return {value: hit._source.sources.length, body: hit._source.body, country: hit._source.body && hit._source.body.address ? hit._source.body.address.country : null};
				});
				cb(null, {similar: similar});
			});
		};

		id = Utils.validateId(id);
		if (!id) {
			return cb(404);
		}
		this.store.Buyer.get(id, (err, result) => {
			if (err) {
				return cb(err);
			}
			if (result.hits.hits.length === 0) {
				return cb(404);
			}
			let company = result.hits.hits[0]._source;
			searchSimilarAuthorityName(company.body.name, [id]);
		});
	}

	getCompany(id, country_id, cb) {
		id = Utils.validateId(id);
		if (!id) {
			return cb(404);
		}
		this.store.Supplier.get(id, (err, result) => {
			if (err) {
				return cb(err);
			}
			if (result.hits.hits.length === 0) {
				return cb(404);
			}
			cb(null, {company: result.hits.hits[0]._source});
		});
	}

	getAuthority(id, country_id, cb) {
		id = Utils.validateId(id);
		if (!id) {
			return cb(404);
		}
		this.store.Buyer.get(id, (err, result) => {
			if (err) {
				return cb(err);
			}
			if (result.hits.hits.length === 0) {
				return cb(404);
			}
			cb(null, {authority: result.hits.hits[0]._source});
		});
	}

	/**
	 init
	 */

	init(cb) {
		this.store.init(err => {
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

}

module.exports = Api;
