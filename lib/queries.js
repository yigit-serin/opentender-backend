const Utils = require('./utils');


let getAnswerAggreggation = (answer, aggID) => {
	if (!answer || !answer[aggID]) {
		return null;
	}
	return answer[aggID][aggID + '_filter'] ? answer[aggID][aggID + '_filter'] : answer[aggID];
};

let applyNestedFilter = (o, q, p) => {
	if (o.aggregations) {
		Object.keys(o.aggregations).forEach(key => {
			let agg = o.aggregations[key];
			if (agg.nested && agg.nested.path === q.path) {
				let a = {};
				a[key + '_filter'] = {
					filter: q.query,
					aggregations: agg.aggregations
				};
				agg.aggregations = a;
			} else {
				applyNestedFilter(agg, q, p + '/aggregations/' + key);
			}
		});
	}
	Object.keys(o).forEach(okey => {
		if ((okey !== 'aggregations') && (typeof o[okey] === 'object')) {
			applyNestedFilter(o[okey], q, p + '/' + okey);
		}
	});

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

const Aggregations = {
	top_authorities: {
		request: {
			'nested': {
				'path': 'buyers'
			},
			'aggregations': {
				'authorities_nested': {
					'terms': {
						'field': 'buyers.groupId',
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
			let result = {
				count: answer.top_authorities.doc_count,
				top10: []
			};
			let root = getAnswerAggreggation(answer, 'top_authorities');
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
	top_companies: {
		request: {
			'nested': {
				'path': 'lots.bids.bidders'
			},
			'aggregations': {
				'companies_nested': {
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
		},
		parse: (answer) => {
			let result = {
				count: answer.top_authorities.doc_count,
				top10: []
			};
			let root = getAnswerAggreggation(answer, 'top_companies');
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
				result[bucket.key] = bucket.sum_price.value;
			});
			return result;
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
						'divisionscpvs': {
							'terms': {
								'field': 'cpvs.code.divisions',
								'size': 10000
							}
						}
					}
				}
			}
		},
		parse: (answer, library) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'terms_main_cpv_divisions');
			let buckets = root.cpvs_filter.divisionscpvs.buckets;
			buckets.forEach(bucket => {
				result[bucket.key] = {
					name: library.getCPVName(bucket.key, 'EN'),
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
						'groupcpvs': {
							'terms': {
								'field': 'cpvs.code.groups',
								'size': 10000
							}
						}
					}
				}
			}
		},
		parse: (answer, library) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'terms_main_cpv_groups');
			let buckets = root.cpvs_filter.groupcpvs.buckets;
			buckets.forEach(bucket => {
				result[bucket.key] = {
					name: library.getCPVName(bucket.key, 'EN'),
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
									{'term': {'lots.bids.isWinning': true}}
								]
							}
						}
					},
					'aggregations': {
						'categoriescpvs': {
							'terms': {
								'field': 'cpvs.code.categories',
								'size': 10000
							}
						}
					}
				}
			}
		},
		parse: (answer, library) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'terms_main_cpv_categories');
			let buckets = root.cpvs_filter.categoriescpvs.buckets;
			buckets.forEach(bucket => {
				result[bucket.key] = {
					name: library.getCPVName(bucket.key, 'EN'),
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
									{'term': {'lots.bids.isWinning': true}}
								]
							}
						}
					},
					'aggregations': {
						'cpvs_full': {
							'terms': {
								'field': 'cpvs.code',
								'size': 10000
							}
						}
					}
				}
			}
		},
		parse: (answer, library) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'terms_main_cpv_full');
			let buckets = root.cpvs_filter.cpvs_full.buckets;
			buckets.forEach(bucket => {
				result[bucket.key] = {
					name: library.getCPVName(bucket.key, 'EN'),
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
	terms_indicators: {
		request: {
			'terms': {
				'field': 'indicators.type'
			}
		},
		parse: (answer) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'terms_indicators');
			let buckets = root.buckets;
			buckets.forEach(bucket => {
				result[bucket.key] = bucket.doc_count;
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
		parse: (answer) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'terms_company_nuts');
			let buckets = root.company_nuts_nested.buckets;
			buckets.sort((a, b) => {
				if (a.key < b.key) return -1;
				if (a.key > b.key) return 1;
				return 0;
			});
			buckets.forEach(bucket => {
				let nut = bucket.key.split('-')[0].trim();
				result[nut] = bucket.doc_count;
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
		parse: (answer) => {
			let result = {};
			let root = getAnswerAggreggation(answer, 'terms_authority_nuts');
			let buckets = root.authority_nuts_nested.buckets;
			buckets.sort((a, b) => {
				if (a.key < b.key) return -1;
				if (a.key > b.key) return 1;
				return 0;
			});
			buckets.forEach(bucket => {
				let nut = bucket.key.split('-')[0].trim();
				result[nut] = bucket.doc_count;
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
									},
								}
							}
						}
					}
				}
			}
		},
		parse: (answer, library, tendercount) => {
			let root = getAnswerAggreggation(answer, 'count_lots_bids');
			let lotsbids = root.lotsbids;
			let result = {
				bids: lotsbids.doc_count,
				bids_awarded: lotsbids.lotsbids_nested_filter.lotsbids_nested.doc_count,
				lots: root.doc_count,
				tenders: tendercount
			};
			return result;
		}
	}
};

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

	let parse = (answer, library, tendercount) => {
		let result = {};
		aggs.forEach(agg => {
			result[agg.id] = agg.parse(answer, library, tendercount);
		});
		return result;
	};

	return {
		request, parse
	};
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

let buildSearchFilter = filter => {

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
					if (isNaN(value)) return null;
					return value;
				});
			} else if (f.mode === '<') {
				return buildQuery(f, 'range', (v) => {
					let value = parseFloat(v);
					if (isNaN(value)) return null;
					return {lt: value};
				});
			} else if (f.mode === '>') {
				return buildQuery(f, 'range', (v) => {
					let value = parseFloat(v);
					if (isNaN(value)) return null;
					return {gt: value};
				});
			}
			return null;
		} else if (f.type === 'range') {
			let m = {range: {}};
			m.range[f.field] = {
				gte: new Date(f.value[0], 0, 1).valueOf(),
				lt: new Date(f.value[1], 0, 1).valueOf()
			};
			return m;
		} else {
			console.log('unknown search filter type:', f.type);
		}
		return null;
	};

	let subfilters = [];

	if (filter.and) {
		subfilters = filter.and.map(and => {
			return buildSearchFilterInternal(and);
		});
	}
	let f = buildSearchFilterInternal(filter);
	if (!f) return null;
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


let buildSearchBody = options => {
	let body = {
		query: {
			match_all: {}
		},
		sort: {
			'modified': {
				'order': 'desc'
			}
		}
	};
	if (options.sort && options.sort.field) {
		body.sort = {};
		body.sort[options.sort.field] = {
			'order': options.sort.ascend ? 'asc' : 'desc',
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

let buildCountrySearchBody = (options, country_id) => {
	let body = buildSearchBody(options);
	if (country_id) {
		if (body.query.match_all) {
			body.query = {
				'bool': {
					'filter': [
						{'term': {'country': country_id}}
					]
				}
			};
		} else if (body.query.bool && body.query.bool.filter) {
			body.query.bool.filter.push({'term': {'country': country_id}});
		} else {
			console.log('unknown search body format', body);
		}
	}
	return body;
};

let buildSearchAggregations = options => {
	let result = null;
	let resolveAgg = (agg, node) => {
		if (!agg.field) return;

		let nested = Utils.getNestedField(agg.field);
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

const Filters = {};

Filters.all = () => {
	return {
		match_all: {}
	};
};

Filters.byBuyers = (buyerIds) => {
	return {
		'nested': {
			'path': 'buyers',
			'query': {
				'bool': {
					'filter': [
						{'terms': {'buyers.groupId': buyerIds}}
					]
				}
			}
		}
	};
};

Filters.byBidders = (bidderIds) => {
	return {
		'nested': {
			'path': 'lots.bids',
			'query': {
				'nested': {
					'path': 'lots.bids.bidders',
					'query': {
						'bool': {
							'filter': [
								{'terms': {'lots.bids.bidders.groupId': bidderIds}}
							]
						}
					}
				}
			}
		}
	};
};

Filters.byAuthorityNuts = (nutscode, level) => {
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
};

Filters.byMainCPV = (cpv, level) => {
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
module.exports.addCountryFilter = addCountryFilter;
module.exports.applyNestedFilterToAggregations = applyNestedFilterToAggregations;
module.exports.compactAggregations = compactAggregations;
module.exports.buildCountrySearchBody = buildCountrySearchBody;
module.exports.buildSearchBody = buildSearchBody;
module.exports.buildSearchAggregations = buildSearchAggregations;
module.exports.buildAggregations = buildAggregations;