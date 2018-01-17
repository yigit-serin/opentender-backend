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

	getAggregations(country_id, aggregation_ids, lang, cb) {
		let aggs = Queries.buildAggregations(aggregation_ids);
		this.store.Tender.aggregations(country_id, aggs.request, (err, result) => {
			if (err) {
				console.error(err);
				return cb(err);
			}
			let stats = aggs.parse(result.aggregations, {library: this.library, lang: lang});
			cb(null, stats);
		});
	}

	getAggregation(country_id, aggregation_id, lang, cb) {
		this.getAggregations(country_id, [aggregation_id], lang, (err, stats) => {
			if (err) {
				return cb(err);
			}
			cb(null, stats[aggregation_id]);
		});
	}

	getIndicatorScoreStats(options, country_id, cb) {
		let indicator = null;
		let isSubindicator = false;

		let indicatorFilter = (options.filters || []).find(f => f.field === 'ot.scores.type' || f.field === 'indicators.type');
		if (indicatorFilter) {
			indicator = indicatorFilter && indicatorFilter.value ? indicatorFilter.value[0] : '';
			isSubindicator = indicatorFilter && indicatorFilter.field === 'indicators.type';
		}
		if (!indicator) {
			return cb('Invalid Parameters');
		}

		let b = Queries.buildCountrySearchBody(options, country_id);

		let aggsConfig = [
			'histogram',
			'terms_main_cpv_divisions',
			{id: 'terms_indicators_score_para', name: indicator}
		];
		if (!isSubindicator) {
			let customWeights = {};
			let weightFilter = (options.filters || []).find(f => f.weights);
			if (weightFilter) {
				Object.keys(weightFilter.weights).forEach(key => {
					let val = weightFilter.weights[key];
					if (!isNaN(val)) {
						customWeights[key] = val;
					}
				});
			}
			if (Object.keys(customWeights).length > 0) {
				aggsConfig.push({id: 'avg_score_custom_para', name: indicator, custom: customWeights});
				aggsConfig.push({id: 'histogram_indicators_custom_para', name: indicator, custom: customWeights});
				aggsConfig.push({id: 'terms_main_cpv_divisions_score_custom_para', name: indicator, custom: customWeights});
			} else {
				aggsConfig.push({id: 'avg_score_para', name: indicator});
				aggsConfig.push({id: 'histogram_indicators_para', name: indicator});
				aggsConfig.push({id: 'terms_main_cpv_divisions_score_para', name: indicator});
			}
		} else {
			aggsConfig.push({id: 'avg_indicator_para', name: indicator});
			aggsConfig.push({id: 'histogram_indicator_para', name: indicator});
			aggsConfig.push({id: 'terms_main_cpv_divisions_indicator_para', name: indicator});
		}

		let aggs = Queries.buildAggregations(aggsConfig);
		b.aggregations = aggs.request;
		this.store.Tender.search(b, 0, 0, (err, result) => {
			if (err) {
				console.error(err);
				return cb(err);
			}
			let data = aggs.parse(result.aggregations, {library: this.library, lang: options.lang});
			let stats = {};
			stats.histogram_indicators = data.histogram_indicators_para || data.histogram_indicator_para || data.histogram_indicators_custom_para;
			stats.terms_main_cpv_divisions_score = data.terms_main_cpv_divisions_indicator_para || data.terms_main_cpv_divisions_score_para || data.terms_main_cpv_divisions_score_custom_para;
			stats.terms_indicators_score = data.terms_indicators_score_para;
			stats.terms_score = data.avg_score_para || data.avg_score_custom_para;
			cb(null, stats);
		});
	}

	getIndicatorRangeStats(options, country_id, cb) {
		let indicator = null;

		let indicatorFilter = (options.filters || []).find(f => f.field === 'ot.scores.type' || f.field === 'indicators.type');
		if (indicatorFilter) {
			indicator = indicatorFilter && indicatorFilter.value ? indicatorFilter.value[0] : '';
		} else {
			indicatorFilter = (options.filters || []).find(f => f.type === 'weighted');
			if (indicatorFilter) {
				indicator = indicatorFilter.field;
			}
		}
		if (!indicator) {
			return cb('Invalid Parameters');
		}
		let aggs = Queries.buildAggregations([
			'terms_main_cpv_divisions',
			'top_terms_companies',
			'top_terms_authorities',
			'top_sum_finalPrice_companies',
			'top_sum_finalPrice_authorities',
			'histogram_finalPriceEUR'
		]);

		let b = Queries.buildCountrySearchBody(options, country_id);

		b.aggregations = aggs.request;

		if (b.query.bool && b.query.bool.filter) {
			Queries.applyNestedFilterToAggregations(b.aggregations, b.query.bool.filter);
		}

		this.store.Tender.search(b, 0, 0, (err, result) => {
			if (err) {
				console.error(err);
				return cb(err);
			}
			let stats = aggs.parse(result.aggregations, {library: this.library, lang: options.lang});

			// now get the some aggregations again without indicator filter, so averages can be calculated
			if (b.query.bool && b.query.bool.filter) {
				b.query.bool.filter = b.query.bool.filter.filter(filter => {
					return (!(filter.nested && ['indicators', 'ot.scores'].indexOf(filter.nested.path) >= 0)) && (filter.type !== 'weighted');
				});
				if (b.query.bool.filter.length === 0) {
					b.query = {match_all: {}};
				}
			}

			aggs = Queries.buildAggregations(['histogram', 'terms_main_cpv_divisions']);
			b.aggregations = aggs.request;

			this.store.Tender.search(b, 0, 0, (err, full_result) => {
				if (err) {
					console.error(err);
					return cb(err);
				}
				let data = aggs.parse(full_result.aggregations, {library: this.library, lang: options.lang});

				let histo_pc_per_year = {};
				let stats_histogram = stats.histogram_finalPriceEUR;
				let data_histogram = data.histogram;
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
				stats.histogram_finalPriceEUR = undefined;
				stats.histogram_count_finalPrices = histo_pc_per_year;

				let terms_pc_main_cpv_divisions = {};
				let stats_terms_main_cpvs = stats.terms_main_cpv_divisions;
				let data_terms_main_cpvs = data.terms_main_cpv_divisions;
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
		let aggs = Queries.buildAggregations([
			'terms_main_cpv_divisions',
			'terms_main_cpv_divisions_scores',
			'terms_procedure_type',
			'top_terms_companies',
			'top_terms_authorities',
			'top_sum_finalPrice_companies',
			'top_sum_finalPrice_authorities',
			'terms_authority_nuts',
			'histogram_indicators',
			'histogram_finalPriceEUR',
			'histogram']);
		let cpv_stats_aggs = Queries.buildAggregations(['sum_finalPriceEUR']);
		let nuts_stats_aggs = Queries.buildAggregations(['sum_finalPriceEUR', 'avg_finalPriceEUR']);
		aggs.request['terms_main_cpv_divisions'].aggregations = cpv_stats_aggs.request;
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
				console.error(err);
				return cb(err);
			}

			let sectors_stats = data.aggregations['terms_main_cpv_divisions'].buckets.map(bucket => {
				return {
					sector: {
						id: bucket.key,
						name: this.library.getCPVName(bucket.key, options.lang),
						valid: this.library.isKnownCPV(bucket.key),
						value: bucket.doc_count
					},
					stats: cpv_stats_aggs.parse(bucket, {library: this.library, lang: options.lang})
				};
			});
			let region_stats = data.aggregations['terms_authority_nuts'].authority_nuts_nested.buckets.map(bucket => {
				return {
					id: bucket.key,
					value: bucket.doc_count,
					stats: nuts_stats_aggs.parse(bucket.tender_denested, {library: this.library, lang: options.lang})
				};
			});
			let stats = aggs.parse(data.aggregations, {library: this.library, lang: options.lang});

			stats.terms_main_cpv_divisions_score = {};
			if (stats.terms_main_cpv_divisions_scores) {
				Object.keys(stats.terms_main_cpv_divisions_scores).forEach(key => {
					let part = stats.terms_main_cpv_divisions_scores[key];
					if (part.scores['TENDER'] !== null) {
						stats.terms_main_cpv_divisions_score[key] = {
							name: part.name,
							value: part.scores['TENDER']
						};
					}
				});
			}

			stats.terms_main_cpv_divisions_scores = undefined;
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
		let levelQuery = Queries.Filters.byMainCPV(cpvInfo.cpv.id, cpvInfo.cpv.level);

		if (b.query.bool) {
			b.query.bool.filter.push(levelQuery);
		} else {
			b.query = levelQuery;
		}

		let aggIds = [
			'top_terms_companies',
			'top_terms_authorities',
			'top_sum_finalPrice_companies',
			'top_sum_finalPrice_authorities',
			'histogram_finalPriceEUR',
			'histogram_indicators',
			'terms_procedure_type',
			'terms_authority_nuts'
		];
		let subaggregationId = null;
		if (cpvInfo.sublevel) {
			subaggregationId = 'terms_main_cpv_' + cpvInfo.sublevel;
			aggIds.push(subaggregationId);
			aggIds.push('terms_main_cpv_' + cpvInfo.sublevel + '_scores');
		}
		let aggs = Queries.buildAggregations(aggIds);
		let stats_aggs = Queries.buildAggregations(['sum_finalPriceEUR']);

		if (subaggregationId) {
			aggs.request[subaggregationId].aggregations = stats_aggs.request;
		}

		b.aggregations = aggs.request;

		if (b.query.bool && b.query.bool.filter) {
			Queries.applyNestedFilterToAggregations(b.aggregations, b.query.bool.filter);
		}
		this.store.Tender.search(b, 0, 0, (err, data) => {
			if (err) {
				return cb(err);
			}
			let stats = aggs.parse(data.aggregations, {library: this.library, lang: options.lang});

			if (subaggregationId) {
				let answer = Queries.getAnswerAggreggation(data.aggregations, subaggregationId);
				let result = answer.buckets.map(bucket => {
					let sector = {
						id: bucket.key,
						name: this.library.getCPVName(bucket.key, options.lang),
						valid: this.library.isKnownCPV(bucket.key),
						value: bucket.doc_count
					};
					return {
						sector: sector,
						stats: stats_aggs.parse(bucket, {library: this.library, lang: options.lang})
					};
				});
				stats[subaggregationId] = undefined;
				stats.sectors_stats = result;
			}

			// now get the some aggregations again without cpv filter, so averages can be calculated
			if (b.query.bool && b.query.bool.filter) {
				b.query.bool.filter = b.query.bool.filter.filter(filter => {
					return (filter !== levelQuery);
				});
				if (b.query.bool.filter.length === 0) {
					b.query = {match_all: {}};
				}
			} else if (b.query === levelQuery) {
				b.query = {match_all: {}};
			}
			aggs = Queries.buildAggregations(['histogram']);
			b.aggregations = aggs.request;

			this.store.Tender.search(b, 0, 0, (err, full_result) => {
				if (err) {
					console.error(err);
					return cb(err);
				}
				let full_data = aggs.parse(full_result.aggregations, {library: this.library, lang: options.lang});

				let histo_pc_per_year = {};
				let stats_histogram = stats.histogram_finalPriceEUR;
				let data_histogram = full_data.histogram;
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
				stats.histogram_finalPriceEUR = undefined;
				stats.histogram_count_finalPrices = histo_pc_per_year;

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
			'histogram']);
		if (nutsInfo.nuts.level < 3) {
			let term = {};
			let field = 'buyers.address.ot.nutscode.nuts' + nutsInfo.nuts.level;
			term[field] = nutsInfo.nuts.id;
			aggs.request.terms_subregions = {
				'nested': {
					'path': 'buyers'
				},
				'aggregations': {
					'buyers_filter': {
						'filter': {
							'query': {
								'bool': {
									'filter': [
										{'term': term},
									]
								}
							}
						},
						'aggregations': {
							'nuts': {
								'terms': {
									'field': 'buyers.address.ot.nutscode.nuts' + (nutsInfo.nuts.level + 1),
									'size': 300000
								},
								'aggregations': {
									'tenders': {
										'reverse_nested': {}
									}
								}
							}
						}
					}
				}
			};
		}

		b.aggregations = aggs.request;
		this.store.Tender.search(b, 0, 0, (err, result) => {
			if (err) {
				return cb(err);
			}
			let stats = aggs.parse(result.aggregations, {library: this.library, lang: options.lang});
			let children = [];
			if (result.aggregations.terms_subregions) {
				stats.terms_subregions_nuts = {};
				if (nutsInfo.nuts.level < 3) {
					let buckets = result.aggregations.terms_subregions.buyers_filter.nuts.buckets;
					buckets.forEach(bucket => {
						let code = bucket.key.toUpperCase();
						if (code !== nutsInfo.nuts.id) {
							stats.terms_subregions_nuts[code] = bucket.tenders.doc_count;
							children.push({
								id: code,
								name: this.library.getNUTSName(code)
							});
						}
					});
				}
			} else {
				stats.terms_subregions_nuts = {};
				stats.terms_subregions_nuts[nutsInfo.nuts.id] = result.hits.total;
			}
			this.fillAggregationBodies(stats, (err) => {
				if (err) {
					return cb(err);
				}
				cb(null, {region: nutsInfo.nuts, parents: nutsInfo.parents, children: children, stats: stats});
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
		let subaggs = Queries.buildAggregations(['terms_authority_nuts', 'top_terms_authorities', 'top_sum_finalPrice_authorities', 'histogram_finalPriceEUR', 'histogram_indicators']);
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

		this.store.Tender.search(b, 0, 0, (err, result) => {
			if (err) {
				return cb(err);
			}
			let stats = aggs.parse(result.aggregations, {library: this.library, lang: options.lang});
			let answer_winning = subaggs.parse(result.aggregations.lotsbids.lotsbids_winning_nested_filter.lotsbids_nested.lotsbids_nested_filter.tender_denested, {library: this.library, lang: options.lang});
			Object.keys(answer_winning).forEach(key => {
				stats[key] = answer_winning[key];
			});
			this.fillAggregationBodies(stats, (err) => {
				if (err) {
					return cb(err);
				}
				let benchmark_aggs = Queries.buildAggregations([
					'histogram_finalPriceEUR',
					'histogram_indicators'
				]);
				let benchmark_q = Queries.buildCountrySearchBody({filters: options.filters}, country_id);
				let benchmark_body = {query: benchmark_q.query, aggregations: benchmark_aggs.request};
				this.store.Tender.search(benchmark_body, 0, 0, (err, benchmark_result) => {
					let benchmark = benchmark_aggs.parse(benchmark_result.aggregations, {library: this.library, lang: options.label});
					stats.benchmark = benchmark;
					cb(null, {stats: stats});
				});
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
			'histogram_finalPriceEUR',
			'histogram_indicators',
			'terms_indicators_score'
		]);
		b.aggregations = aggs.request;
		this.store.Tender.search(b, 0, 0, (err, result) => {
			if (err) {
				console.error(err);
				return cb(err);
			}
			let stats = aggs.parse(result.aggregations, {library: this.library, lang: options.lang});

			this.fillAggregationBodies(stats, (err) => {
				if (err) {
					return cb(err);
				}
				let benchmark_q = Queries.buildCountrySearchBody({filters: options.filters}, country_id);
				let benchmark_aggs = Queries.buildAggregations([
					'histogram_finalPriceEUR',
					'histogram_indicators'
				]);
				let benchmark_body = {query: benchmark_q.query, aggregations: benchmark_aggs.request};
				this.store.Tender.search(benchmark_body, 0, 0, (err, benchmark_result) => {
					let benchmark = benchmark_aggs.parse(benchmark_result.aggregations, {library: this.library, lang: options.label});
					stats.benchmark = benchmark;
					cb(null, {stats: stats});
				});
			});
		});
	}

	getCPVUsageStats(country_id, cb) {
		this.getAggregation(country_id, 'terms_main_cpv_divisions', 'en', cb);
	}

	getCountriesStats(cb) {
		this.getAggregation(null, 'terms_countries', 'en', cb);
	}

	getCompanyNutsStats(country_id, cb) {
		this.getAggregation(country_id, 'terms_company_nuts', 'en', cb);
	}

	getAuthorityNutsStats(country_id, cb) {
		this.getAggregation(country_id, 'terms_authority_nuts', 'en', cb);
	}

	getHomeStats(country_id, cb) {
		this.getAggregations(country_id, ['histogram'], 'en', cb);
	}

	getTenderStats(options, country_id, cb) {
		let benchmark_aggs = Queries.buildAggregations([
			'histogram_distribution_indicators'
		]);
		let benchmark_q = Queries.buildCountrySearchBody({filters: options.filters}, country_id);
		let benchmark_body = {query: benchmark_q.query, aggregations: benchmark_aggs.request};
		this.store.Tender.search(benchmark_body, 0, 0, (err, benchmark_result) => {
			let benchmark = benchmark_aggs.parse(benchmark_result.aggregations, {library: this.library, lang: options.label});
			cb(null, {stats: benchmark});
		});
	}

	/**
	 all dbs
	 */

	autocomplete(entity, field, search, country_id, cb) {
		let index;
		let country_term;
		if (entity === 'tender') {
			index = this.store.Tender;
			country_term = {term: {'ot.country': country_id}};
		} else if (entity === 'company') {
			index = this.store.Supplier;
			country_term = {term: {'countries': country_id}};
		} else if (entity === 'authority') {
			index = this.store.Buyer;
			country_term = {term: {'countries': country_id}};
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
					filter: []
				}
			},
			aggregations: aggfield
		};
		if (country_id) {
			body.query.bool.filter.push(country_term);
		}
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
					console.error('warning', 'multiple tender found for one id', id);
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

	streamTender(id, req, res, options, country_id, onEnd) {
		res.setHeader('Content-Type', 'application/gzip');
		res.setHeader('Cache-Control', 'no-cache');
		res.setHeader('Content-disposition', 'attachment; filename=' + id + '.tenders.json.gz');

		const stream = zlib.createGzip();

		stream.pipe(res);

		let closed = false;

		let close = () => {
			if (!closed) {
				closed = true;
				onEnd && onEnd();
			}
		};

		req.connection.on('close', () => {
			close();
		});

		stream.on('close', () => {
			res.end();
			close();
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
				cpv.valid = this.library.isKnownCPV(cpv.code);
				cpv.name = this.library.getCPVName(cpv.code, lang);
			});
		}
		if (tender.ot.cpv) {
			tender.ot.cpv_name = this.library.getCPVName(tender.ot.cpv, lang);
		}
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
					'ot.country': country_id
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
			body = Queries.addCountrySearchBody(body, {'countries': country_id});
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
			body = Queries.addCountrySearchBody(body, {'countries': country_id});
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
						console.error('alarm', 'body not found', entry);
					}
					entry.id = undefined;
				});
			}
			if (stats.top_terms_companies) {
				stats.top_terms_companies.top10.forEach(entry => {
					entry.body = results[entry.id] || {};
					if (!entry.body.id) {
						console.error('alarm', 'body not found', entry);
					}
					entry.id = undefined;
				});
			}
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
				body.query.bool.must.push({'term': {'countries': country_id}});
			}
			this.store.Supplier.search(body, 200, 0, (err, results) => {
				if (err) {
					return cb(err);
				}
				let similar = results.hits.hits.map(hit => {
					return {value: hit._source.count, body: hit._source.body, country: hit._source.body && hit._source.body.address ? hit._source.body.address.country : null};
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
				body.query.bool.must.push({'term': {'countries': country_id}});
			}
			this.store.Buyer.search(body, 200, 0, (err, results) => {
				if (err) {
					return cb(err);
				}
				let similar = results.hits.hits.map(hit => {
					return {value: hit._source.count, body: hit._source.body, country: hit._source.body && hit._source.body.address ? hit._source.body.address.country : null};
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

	test2(s, cb) {
		let all_weights = {
			TRANSPARENCY: {
				"TRANSPARENCY_NUMBER_OF_KEY_MISSING_FIELDS": 1
			},
			ADMINISTRATIVE: {
				"ADMINISTRATIVE_CENTRALIZED_PROCUREMENT": 1,
				"ADMINISTRATIVE_ELECTRONIC_AUCTION": 1,
				"ADMINISTRATIVE_COVERED_BY_GPA": 1,
				"ADMINISTRATIVE_FRAMEWORK_AGREEMENT": 1,
				"ADMINISTRATIVE_ENGLISH_AS_FOREIGN_LANGUAGE": 1,
			},
			CORRUPTION: {
				"CORRUPTION_SINGLE_BID": 1,
				"CORRUPTION_CALL_FOR_TENDER_PUBLICATION": 1,
				"CORRUPTION_ADVERTISEMENT_PERIOD": 1,
				"CORRUPTION_PROCEDURE_TYPE": 1,
				"CORRUPTION_DECISION_PERIOD": 1,
				"CORRUPTION_TAX_HAVEN": 1,
				"CORRUPTION_NEW_COMPANY": 1,
			}
		};

		let indicator_name = 'ADMINISTRATIVE';
		let indicator_weights = all_weights[indicator_name];
		let fields = Object.keys(indicator_weights).map(key => '_indicators.' + key);
		let weights = Object.values(indicator_weights);
		console.time(s + ' took');
		let b = {
			"query": {
				// "match_all": {},
				"bool": {
					"filter": {
						"script": {
							"params": {
								gte: 10,
								lte: 40,
								fields: fields,
								weights: weights
							},
							"lang": "native",
							"script": "weighted_avg_range"
						}
					}
				}
			},
			// "fields": [
			// 	"_source"
			// ],
			// "script_fields": {
			// 	"test1": {
			// 		"params": {},
			// 		"lang": "native",
			// 		"script": "weighted_avg_range"
			// 	}
			// },
		};

		this.store.Tender.search(b, 1, 0, (err, result) => {
			console.timeEnd(s + ' took');
			if (err) {
				return console.log(err);
			}
			result.hits.hits.forEach(hit => {
				console.log(JSON.stringify({id: hit}));
			});
			cb && cb();
		})
	}


	test(s, cb) {
		let all_weights = {
			TRANSPARENCY: {
				"TRANSPARENCY_NUMBER_OF_KEY_MISSING_FIELDS": 1
			},
			ADMINISTRATIVE: {
				"ADMINISTRATIVE_CENTRALIZED_PROCUREMENT": 0.5,
				"ADMINISTRATIVE_ELECTRONIC_AUCTION": 0.5,
				"ADMINISTRATIVE_COVERED_BY_GPA": 0.5,
				"ADMINISTRATIVE_FRAMEWORK_AGREEMENT": 0.5,
				"ADMINISTRATIVE_ENGLISH_AS_FOREIGN_LANGUAGE": 0.5,
			},
			CORRUPTION: {
				"CORRUPTION_SINGLE_BID": 1,
				"CORRUPTION_CALL_FOR_TENDER_PUBLICATION": 1,
				"CORRUPTION_ADVERTISEMENT_PERIOD": 1,
				"CORRUPTION_PROCEDURE_TYPE": 1,
				"CORRUPTION_DECISION_PERIOD": 1,
				"CORRUPTION_TAX_HAVEN": 1,
				"CORRUPTION_NEW_COMPANY": 1,
			}
		};

		let indicator_name = 'ADMINISTRATIVE';
		let indicator_weights = all_weights[indicator_name];
		let b = {
			"query": {
				"match_all": {},
				// 'bool': {
				// 	'filter': [
				// 		{'terms': {'id': ['00afd2ee-a8c7-49e9-8015-5f9c436effff', "00b32d2e-cdba-4cee-8541-578f491f66bd"]}}
				// 	]
				// }
			},
			"aggs": {}
		};

		if (s === 'native-avg') {
			b.aggs.native = {
				'nested': {
					'path': 'ot.scores'
				},
				'aggregations': {
					'avg_scores_nested': {
						'terms': {
							'field': 'ot.scores.type',
							'size': 100
						},
						'aggregations': {
							'avg_scores_nested_avg': {
								'avg': {
									'field': 'ot.scores.value'
								}
							}
						}
					}
				}
			}
		}

		if (s === 'nativescript') {
			let fields = Object.keys(indicator_weights).map(key => 'ot.indicators.' + key);
			let weights = Object.values(indicator_weights);
			b.aggs.nativescript = {
				"scripted_metric": {
					"params": {"fields": fields, "weights": weights},
					"init_script": {
						"lang": "native",
						"script": "weighted_avg_init"
					},
					"map_script": {
						"lang": "native",
						"script": "weighted_avg_map"
					},
					"combine_script": {
						"lang": "native",
						"script": "weighted_avg_combine"
					},
					"reduce_script": {
						"lang": "native",
						"script": "weighted_avg_reduce"
					}
				}
			}
		}

		console.time(s + ' took');
		this.store.Tender.search(b, 0, 0, (err, result) => {
			console.timeEnd(s + ' took');
			if (err) {
				return console.log(err);
			}
			cb && cb(result);
		})
	}

	/**
	 init
	 */

	init(cb) {
		this.store.init(err => {
			// this.test('nativescript', (result) => {
			// 	console.log(JSON.stringify(result.aggregations, null, '\t'));
			// 	this.test('native-avg', (result) => {
			// 		console.log(JSON.stringify(result.aggregations.native.avg_scores_nested.buckets.find(b => b.key == 'ADMINISTRATIVE'), null, '\t'));
			// 	});
			//
			// });
			cb(err);
		});
	};


}

module.exports = Api;
