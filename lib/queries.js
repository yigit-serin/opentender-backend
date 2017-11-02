const Utils = require('./utils');
const moment = require('moment');

const Aggregations = {
	top_sum_finalPrice_authorities: {
		request: {
			'nested': {
				'path': 'buyers'
			},
			'aggregations': {
				'authorities_nested': {
					'terms': {
						'field': 'buyers.id',
						'shard_size': 0,
						'size': 10,
						'order': [{'sum_agg>sum_finalPriceEUR': 'desc'}]
					},
					'aggregations': {
						'sum_agg': {
							'reverse_nested': {},
							'aggregations': {
								'sum_finalPriceEUR': {
									'sum': {
										'field': 'finalPrice.netAmountEur'
									}
								}
							}
						}
					}
				}
			}
		},
		parse: (answer) => {
			let root = getAnswerAggreggation(answer, 'top_sum_finalPrice_authorities');
			let result = {
				count: root.doc_count,
				top10: []
			};
			let buckets = root.authorities_nested.buckets;
			buckets.forEach(bucket => {
				if (bucket.sum_agg.sum_finalPriceEUR.value > 0) {
					result.top10.push({
						value: bucket.sum_agg.sum_finalPriceEUR.value,
						id: bucket.key
					});
				}
			});
			return result;
		}
	},
	top_terms_authorities: {
		request: {
			'nested': {
				'path': 'buyers'
			},
			'aggregations': {
				'authorities_nested': {
					'terms': {
						'field': 'buyers.id',
						'size': 10
					}
				}
			}
		},
		parse: (answer) => {
			let root = getAnswerAggreggation(answer, 'top_terms_authorities');
			let result = {
				count: root.doc_count,
				top10: []
			};
			let buckets = root.authorities_nested.buckets;
			buckets.forEach(bucket => {
				result.top10.push({
					value: bucket.doc_count,
					id: bucket.key
				});
			});
			return result;
		}
	},
	top_authorities: {
		request: {
			'nested': {
				'path': 'buyers'
			},
			'aggregations': {
				'authorities_nested': {
					'terms': {
						'field': 'buyers.id',
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
		},
		parse: (answer) => {
			let root = getAnswerAggreggation(answer, 'top_authorities');
			let result = {
				count: root.doc_count,
				top10: []
			};
			let buckets = root.authorities_nested.buckets;
			buckets.forEach(bucket => {
				result.top10.push({
					value: bucket.doc_count,
					body: bucket.hits.hits.hits[0]._source
				});
			});
			return result;
		}
	},
	top_sum_finalPrice_companies: {
		request: {
			'nested': {
				'path': 'lots.bids.bidders'
			},
			'aggregations': {
				'companies_nested': {
					'terms': {
						'field': 'lots.bids.bidders.id',
						'shard_size': 0,
						'size': 10,
						'order': [{'sum_agg>sum_finalPriceEUR': 'desc'}]
					},
					'aggregations': {
						'sum_agg': {
							'reverse_nested': {},
							'aggregations': {
								'sum_finalPriceEUR': {
									'sum': {
										'field': 'finalPrice.netAmountEur'
									}
								}
							}
						}
					}
				}
			}
		},
		parse: (answer) => {
			let root = getAnswerAggreggation(answer, 'top_sum_finalPrice_companies');
			let result = {
				count: root.doc_count,
				top10: []
			};
			let buckets = root.companies_nested.buckets;
			buckets.forEach(bucket => {
				if (bucket.sum_agg.sum_finalPriceEUR.value > 0) {
					result.top10.push({
						value: bucket.sum_agg.sum_finalPriceEUR.value,
						id: bucket.key
					});
				}
			});
			return result;
		}
	},
	top_terms_companies: {
		request: {
			'nested': {
				'path': 'lots.bids.bidders'
			},
			'aggregations': {
				'companies_nested': {
					'terms': {
						'field': 'lots.bids.bidders.id',
						'size': 10
					}
				}
			}
		},
		parse: (answer) => {
			let root = getAnswerAggreggation(answer, 'top_terms_companies');
			let result = {
				count: root.doc_count,
				top10: []
			};
			let buckets = root.companies_nested.buckets;
			buckets.forEach(bucket => {
				result.top10.push({
					value: bucket.doc_count,
					id: bucket.key
				});
			});
			return result;
		}
	},
	top_companies: {
		request: {
			'nested': {
				'path': 'lots.bids.bidders'
			},
			'aggregations': {
				'companies_nested': {
					'terms': {
						'field': 'lots.bids.bidders.id',
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
		},
		parse: (answer) => {
			let root = getAnswerAggreggation(answer, 'top_companies');
			let result = {
				count: root.doc_count,
				top10: []
			};
			let buckets = root.companies_nested.buckets;
			buckets.forEach(bucket => {
				result.top10.push({
					value: bucket.doc_count,
					body: bucket.hits.hits.hits[0]._source
				});
			});
			return result;
		}
	},
	top_winning_companies: {
		request: {
			'nested': {
				'path': 'lots.bids'
			},
			'aggregations': {
				'top_winning_companies_nested_filter': {
					'filter': {
						'query': {
							'bool': {
								'filter': [
									{'term': {'lots.bids.isWinning': true}}
								]
							}
						}
					},
					'aggregations': {
						'companies': {
							'nested': {
								'path': 'lots.bids.bidders'
							},
							'aggregations': {
								'companies_nested': {
									'terms': {
										'field': 'lots.bids.bidders.id',
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
		parse: (answer) => {
			let result = {
				count: answer.top_winning_companies.doc_count,
				top10: []
			};
			let root = getAnswerAggreggation(answer, 'top_winning_companies');
			let buckets = root.top_winning_companies_nested_filter.companies.companies_nested.buckets;
			buckets.forEach(bucket => {
				result.top10.push({
					value: bucket.doc_count,
					body: bucket.hits.hits.hits[0]._source
				});
			});
			return result;
		}
	},
	sums_finalPrice: {
		request: {
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
		parse: (answer) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'sums_finalPrice');
			let buckets = root.buckets;
			buckets.forEach(bucket => {
				result[bucket.key] = Utils.roundValueTwoDecimals(bucket.sum_price.value);
			});
			return result;
		}
	},
	sum_finalPriceEUR: {
		request: {
			'sum': {
				'field': 'finalPrice.netAmountEur'
			}
		},
		parse: (answer) => {
			return getAnswerAggreggation(answer, 'sum_finalPriceEUR');
		}
	},
	avg_finalPriceEUR: {
		request: {
			'avg': {
				'field': 'finalPrice.netAmountEur'
			}
		},
		parse: (answer) => {
			return getAnswerAggreggation(answer, 'avg_finalPriceEUR');
		}
	},
	terms_main_cpv_divisions: {
		request: {
			'nested': {
				'path': 'cpvs'
			},
			'aggregations': {
				'cpvs_filter': {
					'filter': {
						'query': {
							'bool': {
								'filter': [
									{'term': {'cpvs.isMain': true}}
								]
							}
						}
					},
					'aggregations': {
						'cpvs': {
							'terms': {
								'field': 'cpvs.code.divisions',
								'size': 10000
							}
						}
					}
				}
			}
		},
		parse: (answer, options) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'terms_main_cpv_divisions');
			let buckets = root.cpvs_filter.cpvs.buckets;
			buckets.forEach(bucket => {
				result[bucket.key] = {
					name: options.library.getCPVName(bucket.key, options.lang),
					value: bucket.doc_count
				};
			});
			return result;
		}
	},
	terms_main_cpv_groups: {
		request: {
			'nested': {
				'path': 'cpvs'
			},
			'aggregations': {
				'cpvs_filter': {
					'filter': {
						'query': {
							'bool': {
								'filter': [
									{'term': {'cpvs.isMain': true}}
								]
							}
						}
					},
					'aggregations': {
						'cpvs': {
							'terms': {
								'field': 'cpvs.code.groups',
								'size': 10000
							}
						}
					}
				}
			}
		},
		parse: (answer, options) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'terms_main_cpv_groups');
			let buckets = root.cpvs_filter.cpvs.buckets;
			buckets.forEach(bucket => {
				result[bucket.key] = {
					name: options.library.getCPVName(bucket.key, options.lang),
					value: bucket.doc_count
				};
			});
			return result;
		}
	},
	terms_main_cpv_categories: {
		request: {
			'nested': {
				'path': 'cpvs'
			},
			'aggregations': {
				'cpvs_filter': {
					'filter': {
						'query': {
							'bool': {
								'filter': [
									{'term': {'cpvs.isMain': true}}
								]
							}
						}
					},
					'aggregations': {
						'cpvs': {
							'terms': {
								'field': 'cpvs.code.categories',
								'size': 10000
							}
						}
					}
				}
			}
		},
		parse: (answer, options) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'terms_main_cpv_categories');
			let buckets = root.cpvs_filter.cpvs.buckets;
			buckets.forEach(bucket => {
				result[bucket.key] = {
					name: options.library.getCPVName(bucket.key, options.lang),
					value: bucket.doc_count
				};
			});
			return result;
		}
	},
	terms_main_cpv_full: {
		request: {
			'nested': {
				'path': 'cpvs'
			},
			'aggregations': {
				'cpvs_filter': {
					'filter': {
						'query': {
							'bool': {
								'filter': [
									{'term': {'cpvs.isMain': true}}
								]
							}
						}
					},
					'aggregations': {
						'cpvs': {
							'terms': {
								'field': 'cpvs.code',
								'size': 10000
							}
						}
					}
				}
			}
		},
		parse: (answer, options) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'terms_main_cpv_full');
			let buckets = root.cpvs_filter.cpvs.buckets;
			buckets.forEach(bucket => {
				result[bucket.key] = {
					name: options.library.getCPVName(bucket.key, options.lang),
					value: bucket.doc_count
				};
			});
			return result;
		}
	},
	terms_countries: {
		request: {
			terms: {
				field: 'country',
				size: 10000
			}
		},
		parse: (answer) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'terms_countries');
			let buckets = root.buckets;
			buckets.forEach((bucket) => {
				result[bucket.key.toLowerCase()] = bucket.doc_count;
			});
			return result;
		}
	},
	terms_procedure_type: {
		request: {
			terms: {
				field: 'procedureType',
				size: 100
			}
		},
		parse: (answer) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'terms_procedure_type');
			let buckets = root.buckets;
			buckets.forEach((bucket) => {
				result[bucket.key] = bucket.doc_count;
			});
			return result;
		}
	},
	terms_indicators: {
		request: {
			'nested': {
				'path': 'indicators'
			},
			'aggregations': {
				'terms_indicators_nested': {
					'terms': {
						'field': 'indicators.type',
						'size': 1000
					}
				}
			}
		},
		parse: (answer) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'terms_indicators');
			let buckets = root.terms_indicators_nested.buckets;
			buckets.forEach(bucket => {
				result[bucket.key] = bucket.doc_count;
			});
			return result;
		}
	},
	terms_indicators_score: {
		request: {
			'nested': {
				'path': 'indicators'
			},
			'aggregations': {
				'terms_indicators_score_nested': {
					'terms': {
						'field': 'indicators.type',
						'size': 1000
					},
					'aggregations': {
						'terms_indicators_score_nested_avg': {
							'avg': {
								'field': 'indicators.value'
							}
						}
					}
				}
			}
		},
		parse: (answer) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'terms_indicators_score');
			let buckets = root.terms_indicators_score_nested.buckets;
			buckets.forEach(bucket => {
				if (bucket.terms_indicators_score_nested_avg.value !== null) {
					result[bucket.key] = bucket.terms_indicators_score_nested_avg.value;
				}
			});
			return result;
		}
	},
	avg_scores: {
		request: {
			'nested': {
				'path': 'scores'
			},
			'aggregations': {
				'avg_scores_nested': {
					'terms': {
						'field': 'scores.type'
					},
					'aggregations': {
						'avg_scores_nested_avg': {
							'avg': {
								'field': 'scores.value'
							}
						}
					}
				}
			}
		},
		parse: (answer) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'avg_scores');
			let buckets = root.avg_scores_nested.buckets;
			buckets.forEach(bucket => {
				result[bucket.key] = bucket.avg_scores_nested_avg.value;
			});
			return result;
		}
	},
	terms_company_nuts: {
		request: {
			'nested': {
				'path': 'lots.bids.bidders'
			},
			'aggregations': {
				'company_nuts_nested': {
					'terms': {
						'field': 'lots.bids.bidders.address.nuts',
						'size': 3000000
					}
				}
			}
		},
		parse: (answer, options) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'terms_company_nuts');
			let buckets = root.company_nuts_nested.buckets;
			buckets.sort((a, b) => {
				if (a.key < b.key) {
					return -1;
				}
				if (a.key > b.key) {
					return 1;
				}
				return 0;
			});
			buckets.forEach(bucket => {
				let nut = options.library.mapNut(bucket.key);
				result[nut] = (result[nut] || 0) + bucket.doc_count;
			});
			return result;
		}
	},
	terms_authority_nuts: {
		request: {
			'nested': {
				'path': 'buyers'
			},
			'aggregations': {
				'authority_nuts_nested': {
					'terms': {
						'field': 'buyers.address.nuts',
						'size': 3000000
					}
				}
			}
		},
		parse: (answer, options) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'terms_authority_nuts');
			let buckets = root.authority_nuts_nested.buckets;
			buckets.sort((a, b) => {
				if (a.key < b.key) {
					return -1;
				}
				if (a.key > b.key) {
					return 1;
				}
				return 0;
			});
			buckets.forEach(bucket => {
				let nut = options.library.mapNut(bucket.key);
				result[nut] = (result[nut] || 0) + bucket.doc_count;
			});
			return result;
		}
	},
	histogram_lots_awardDecisionDate: {
		request: {
			'nested': {
				'path': 'lots'
			},
			'aggregations': {
				'dates_nested': {
					'date_histogram': {'field': 'lots.awardDecisionDate', 'interval': 'year'}
				}
			}
		},
		parse: (answer) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'histogram_lots_awardDecisionDate');
			let buckets = root.dates_nested.buckets;
			buckets.forEach(bucket => {
				let year = parseInt(bucket.key_as_string.slice(0, 4), 10);
				if (Utils.isValidDigiwhistYear(year)) {
					result[year] = bucket.doc_count;
				}
			});
			return result;
		}
	},
	histogram_lots_awardDecisionDate_finalPriceEUR: {
		request: {
			'nested': {
				'path': 'lots'
			},
			'aggregations': {
				'dates_nested': {
					'date_histogram': {'field': 'lots.awardDecisionDate', 'interval': 'year'},
					'aggregations': {
						'tender_denested': {
							'reverse_nested': {},
							'aggregations': {
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
		parse: (answer) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'histogram_lots_awardDecisionDate_finalPriceEUR');
			let buckets = root.dates_nested.buckets;
			buckets.forEach(bucket => {
				let year = parseInt(bucket.key_as_string.slice(0, 4), 10);
				if (Utils.isValidDigiwhistYear(year)) {
					let sub_buckets = bucket.tender_denested;
					result[year] = {value: bucket.doc_count, sum_finalPriceEUR: sub_buckets.sum_finalPriceEUR, avg_finalPriceEUR: sub_buckets.avg_finalPriceEUR};
				}
			});
			// console.log(JSON.stringify(result, null, '\t'));
			return result;
		}
	},
	histogram_lots_awardDecisionDate_reverseNested: {
		request: {
			'reverse_nested': {
				'path': 'lots'
			},
			'aggregations': {
				'dates_nested': {
					'date_histogram': {'field': 'lots.awardDecisionDate', 'interval': 'year'}
				}
			}
		},
		parse: (answer) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'histogram_lots_awardDecisionDate_reverseNested');
			let buckets = root.dates_nested.buckets;
			buckets.forEach(bucket => {
				let year = parseInt(bucket.key_as_string.slice(0, 4), 10);
				if (Utils.isValidDigiwhistYear(year)) {
					result[year] = bucket.doc_count;
				}
			});
			return result;
		}
	},
	count_lots_bids: {
		request: {
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
									}
								}
							}
						}
					}
				}
			}
		},
		parse: (answer, options) => {
			let root = getAnswerAggreggation(answer, 'count_lots_bids');
			let lotsbids = root.lotsbids;
			return {
				bids: lotsbids.doc_count,
				bids_awarded: lotsbids.lotsbids_nested_filter.lotsbids_nested.doc_count,
				lots: root.doc_count,
				tenders: options.tendercount
			};
		}
	}
};

const Filters = {
	all: () => {
		return {
			match_all: {}
		};
	},
	byBuyers: (buyerIds) => {
		return {
			'nested': {
				'path': 'buyers',
				'query': {
					'bool': {
						'filter': [
							{'terms': {'buyers.id': buyerIds}}
						]
					}
				}
			}
		};
	},
	byBidders: (bidderIds) => {
		return {
			'nested': {
				'path': 'lots.bids',
				'query': {
					'nested': {
						'path': 'lots.bids.bidders',
						'query': {
							'bool': {
								'filter': [
									{'terms': {'lots.bids.bidders.id': bidderIds}}
								]
							}
						}
					}
				}
			}
		};
	},
	byAuthorityNuts: (nutscode, level) => {
		let term = {};
		let field = 'buyers.address.nuts';
		if (!isNaN(level) && level >= 0 && level < 4) {
			field += '.nuts' + level;
		}
		term[field] = nutscode;
		return {
			'nested': {
				'path': 'buyers',
				'query': {
					'bool': {
						'filter': [
							{'term': term}
						]
					}
				}
			}
		};
	},
	byMainCPV: (cpv, level) => {
		let term = {};
		term['cpvs.code' + (level ? '.' + level : '')] = cpv;
		return {
			'nested': {
				'path': 'cpvs',
				'query': {
					'bool': {
						'filter': [
							{'term': term},
							{'term': {'cpvs.isMain': true}}
						]
					}
				}
			}
		};
	}
};

/*
	adding filter queries to aggregations (and parse them back)
 */

let getAnswerAggreggation = (answer, aggID) => {
	if (!answer || !answer[aggID]) {
		return null;
	}
	return answer[aggID][aggID + '_filter'] ? answer[aggID][aggID + '_filter'] : answer[aggID];
};

let applyNestedFilterToAggregations = (aggregations, filters) => {
	// TODO: "nested: 'lots'" must be applied to "nested: 'lots.bids'" and "nested: 'lots.bids.bidders'" ... too
	Object.keys(aggregations).forEach(key => {
		let agg = aggregations[key];
		if (agg.nested) {
			let nested_filters = filters.filter(filter => {
				return filter.nested && (filter.nested.path === agg.nested.path);
			}).map(filter => {
				return filter.nested.query;
			});
			if (nested_filters.length > 0) {
				// console.log('before applying');
				// console.log(JSON.stringify(agg, null, '\t'));
				let a = {};
				a[key + '_filter'] = {
					filter: {query: {bool: {filter: nested_filters}}},
					aggregations: agg.aggregations
				};
				agg.aggregations = a;
				// console.log('after applying');
				// console.log(JSON.stringify(agg, null, '\t'));
			}
		}
	});
};

/*
	search filter queries & aggregations
 */

let parseSearchAggregations = (aggregations, options) => {
	let resolveNode = n => {
		if (!n) {
			return;
		}
		delete n.doc_count_error_upper_bound;
		delete n.sum_other_doc_count;
		let list = Object.keys(n);
		list.forEach(key => {
			let o = n[key];
			if (key !== 'buckets' && typeof o === 'object') {
				resolveNode(o);
				if (key.indexOf('_nested') > 0) {
					Object.keys(o).forEach(k => {
						n[k] = o[k];
					});
					delete n[key];// = undefined;
				}
			}
			if (key.indexOf('cpvs_code') === 0) {
				o.buckets.forEach(bucket => {
					bucket.name = options.library.getCPVName(bucket.key, options.lang);
				});
			} else if (key.indexOf('lots_awardDecisionDate') === 0) {
				o.buckets = o.buckets.filter(bucket => {
					return bucket.doc_count > 0;
				}).map(bucket => {
					return {
						key: parseInt(bucket.key_as_string.slice(0, 4), 10),
						doc_count: bucket.doc_count
					};
				}).filter(bucket => {
					return Utils.isValidDigiwhistYear(bucket.key);
				});
			} else if (
				(key.indexOf('estimatedStartDate') === 0) ||
				(key.indexOf('estimatedCompletionDate') === 0)
			) {
				o.buckets = o.buckets.filter(bucket => {
					return bucket.doc_count > 0;
				}).map(bucket => {
					return {
						key: parseInt(bucket.key_as_string.slice(0, 4), 10),
						doc_count: bucket.doc_count
					};
				}).filter(bucket => {
					return (bucket.key > 2000 && bucket.key < 2100); // TODO: isValidCentury
				});
			}
		});
	};
	resolveNode(aggregations);
};

let buildSearchFilter = (filter) => {

	let buildQuery = (f, mode, parseValue) => {
		let values = Array.isArray(f.value) ? f.value : [f.value];
		if (values.length > 0) {
			let b = {or: []};
			values.forEach(v => {
				let value = parseValue ? parseValue(v) : v;
				if (value !== null) {
					let m = {};
					m[mode] = {};
					m[mode][f.field] = value;
					b.or.push(m);
				}
			});
			if (b.or.length === 1) {
				return b.or[0];
			}
			if (b.or.length === 0) {
				return null;
			}
			return b;
		}
		return null;
	};

	let buildSearchFilterInternal = f => {
		if (f.type === 'select') {
			return buildQuery(f, 'term');
		} else if (f.type === 'term') {
			return buildQuery(f, 'term');
		} else if (f.type === 'match') {
			return buildQuery(f, 'match');
		} else if (f.type === 'text') {
			return buildQuery(f, 'match_phrase_prefix');
		} else if (f.type === 'value') {
			if (f.mode === '=') {
				return buildQuery(f, 'term', (v) => {
					let value = parseFloat(v);
					return (isNaN(value)) ? null : value;
				});
			} else if (f.mode === '<') {
				return buildQuery(f, 'range', (v) => {
					let value = parseFloat(v);
					return (isNaN(value)) ? null : {lt: value};
				});
			} else if (f.mode === '<=') {
				return buildQuery(f, 'range', (v) => {
					let value = parseFloat(v);
					return (isNaN(value)) ? null : {lte: value};
				});
			} else if (f.mode === '>') {
				return buildQuery(f, 'range', (v) => {
					let value = parseFloat(v);
					return (isNaN(value)) ? null : {gt: value};
				});
			} else if (f.mode === '>=') {
				return buildQuery(f, 'range', (v) => {
					let value = parseFloat(v);
					return (isNaN(value)) ? null : {gte: value};
				});
			}
			return null;
		} else if (f.type === 'date') {
			if (f.mode === '=') {
				return buildQuery(f, 'range', (v) => {
					let value = moment(v);
					return value.isValid ? {gte: value.toDate(), lt: value.add(1, 'd').toDate()} : null;
				});
			} else if (f.mode === '>') {
				return buildQuery(f, 'range', (v) => {
					let value = moment(v);
					return value.isValid ? {gt: value.toDate()} : null;
				});
			} else if (f.mode === '>=') {
				return buildQuery(f, 'range', (v) => {
					let value = moment(v);
					return value.isValid ? {gte: value.toDate()} : null;
				});
			} else if (f.mode === '<') {
				return buildQuery(f, 'range', (v) => {
					let value = moment(v);
					return value.isValid ? {lt: value.toDate()} : null;
				});
			} else if (f.mode === '<=') {
				return buildQuery(f, 'range', (v) => {
					let value = moment(v);
					return value.isValid ? {lte: value.toDate()} : null;
				});
			}
			return null;
		} else if (f.type === 'range') {
			let m = {range: {}};
			m.range[f.field] = {
				gte: new Date(f.value[0], 0, 1).valueOf(),
				lte: new Date(f.value[1], 0, 1).valueOf()
			};
			return m;
		}
		console.log('unknown search filter type:', f.type);
		return null;
	};

	let subfilters = [];
	if (filter.and) {
		subfilters = filter.and.map(and => {
			return buildSearchFilterInternal(and);
		}).filter(f => {
			return f !== null;
		});
	}
	let f = buildSearchFilterInternal(filter);
	if (!f) {
		return null;
	}
	subfilters.push(f);
	let nested = Utils.getNestedField(filter.field);
	let query = f;
	if (subfilters.length > 1) {
		query = {
			'bool': {
				'filter': subfilters
			}
		};
	}
	if (nested) {
		return {
			'nested': {
				'path': nested,
				'query': query
			}
		};
	}
	return query;
};

let buildSearchBody = (options, disableDefaultSort) => {
	let body = {
		query: {
			match_all: {}
		}
	};
	if (!disableDefaultSort) {
		body.sort = {
			'modified': {
				'order': 'desc'
			}
		}
	}

	if (options.sort && options.sort.field) {
		body.sort = {};
		body.sort[options.sort.field] = {
			'order': options.sort.ascend ? 'asc' : 'desc'
		};
		let nested = Utils.getNestedField(options.sort.field);
		if (nested) {
			body.sort[options.sort.field]['nested_path'] = nested;
		}
	}
	body.aggregations = buildSearchAggregations(options) || undefined;
	if (options.filters) {
		let filters = [];
		options.filters.forEach(filter => {
			let f = buildSearchFilter(filter);
			if (f) {
				filters.push(f);
			}
		});
		if (filters.length > 0) {
			body.query = {
				'bool': {
					'filter': filters
				}
			};
		}
	}
	return body;
};

let addCountrySearchBody = (body, term) => {
	if (body.query.match_all) {
		body.query = {
			'bool': {
				'filter': [
					{'term': term}
				]
			}
		};
	} else if (body.query.bool && body.query.bool.filter) {
		body.query.bool.filter.push({'term': term});
	} else {
		console.log('unknown search body format', body);
	}
	return body;
};

let buildCountrySearchBody = (options, country_id) => {
	let body = buildSearchBody(options);
	if (country_id) {
		body = addCountrySearchBody(body, {'country': country_id});
	}
	return body;
};

let buildSearchAggregations = (options) => {
	let result = null;
	let resolveAgg = (agg, node) => {
		if (!agg.field) {
			return;
		}

		let nested = Utils.getNestedField(agg.field);
		if (nested) {
			let aa = {
				'nested': {
					'path': nested
				},
				aggregations: {}
			};
			if (agg.type === 'range') {
				aa.aggregations[agg.field.replace(/\./g, '_') + '_nested'] = {
					'date_histogram': {'field': agg.field, 'interval': 'year'}
				};
			} else {
				aa.aggregations[agg.field.replace(/\./g, '_') + '_nested'] = {
					'terms': {'field': agg.field, size: agg.size || 5}
				};
			}
			node[agg.field.replace(/\./g, '_')] = aa;
			return;
		}

		if (agg.type === 'sum') {
			node[agg.field.replace(/\./g, '_') + '_sum'] = {'sum': {'field': agg.field}};
			return;
		} else if (agg.type === 'top') {
			node[agg.field.replace(/\./g, '_') + '_hits'] = {'top_hits': {'size': 1, _source: {include: [agg.field]}}};
			return;
		} else if (agg.type === 'histogram') {
			node[agg.field.replace(/\./g, '_') + '_over_time'] = {'date_histogram': {'field': agg.field, 'interval': 'year'}};
			return;
		} else if (agg.type === 'value') {
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


/*
	aggreggation utils
 */

let buildAggregations = (aggIds) => {
	let request = {};

	let aggs = aggIds.map(id => {
		if (!Aggregations[id]) {
			console.log('invalid aggregation id', id);
		}
		return {
			id: id,
			request: Utils.clone(Aggregations[id].request),
			parse: Aggregations[id].parse
		};
	});
	aggs.forEach(agg => {
		request[agg.id] = agg.request;
	});

	let parse = (answer, options) => {
		let result = {};
		aggs.forEach(agg => {
			result[agg.id] = agg.parse(answer, options);
		});
		return result;
	};

	return {
		request, parse
	};
};

let addCountryFilter = (query, country_id) => {
	if (!country_id) {
		return query;
	}
	if (query.bool && query.filter) {
		query.filter.push({'term': {'country': country_id}});
		return query;
	}
	return {
		'bool': {
			'filter': [
				query,
				{'term': {'country': country_id}}
			]
		}
	};
};


module.exports.Filters = Filters;
module.exports.Aggregations = Aggregations;
module.exports.addCountrySearchBody = addCountrySearchBody;
module.exports.getAnswerAggreggation = getAnswerAggreggation;
module.exports.addCountryFilter = addCountryFilter;
module.exports.applyNestedFilterToAggregations = applyNestedFilterToAggregations;
module.exports.parseSearchAggregations = parseSearchAggregations;
module.exports.buildCountrySearchBody = buildCountrySearchBody;
module.exports.buildSearchBody = buildSearchBody;
module.exports.buildSearchAggregations = buildSearchAggregations;
module.exports.buildAggregations = buildAggregations;
