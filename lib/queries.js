const Utils = require('./utils');

let nested_objects = ['lots.bids.bidders', 'lots.bids', 'lots.cpvs', 'lots', 'buyers', 'cpvs'];

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
		parse: (answer, cpvs) => {
			let result = {
				count: answer.top_authorities.doc_count,
				top10: []
			};
			answer.top_authorities.authorities_nested.buckets.forEach(bucket => {
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
		parse: (answer, cpvs) => {
			let result = {
				count: answer.top_authorities.doc_count,
				top10: []
			};
			answer.top_companies.companies_nested.buckets.forEach(bucket => {
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
				'top_winning_companies_filter': {
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
		parse: (answer, cpvs) => {
			let result = {
				count: answer.top_winning_companies.doc_count,
				top10: []
			};
			answer.top_winning_companies.top_winning_companies_filter.companies.companies_nested.buckets.forEach(bucket => {
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
		parse: (answer, cpvs) => {
			let result = {};
			answer.sums_finalPrice.buckets.forEach(bucket => {
				result[bucket.key] = bucket.sum_price.value;
			});
			return result;
		}
	},
	terms_main_cpvs: {
		request: {
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
								'field': 'cpvs.code.main',
								'size': 10000
							}
						}
					}
				}
			}
		},
		parse: (answer, cpvs) => {
			let result = {};
			answer.terms_main_cpvs.cpvs_filter.maincpvs.buckets.forEach(bucket => {
				result[bucket.key] = {
					name: cpvs.getCPVName(bucket.key, 'EN'),
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
		parse: (answer, cpvs) => {
			let result = {};
			answer.terms_countries.buckets.forEach((bucket) => {
				result[bucket.key.toLowerCase()] = bucket.doc_count;
			});
			return result;
		}
	},
	terms_main_cpvs_full: {
		request: {
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
								'field': 'cpvs.code',
								'size': 10000
							}
						}
					}
				}
			}
		},
		parse: (answer, cpvs) => {
			let result = {};
			answer.terms_main_cpvs_full.cpvs_filter.maincpvs.buckets.forEach(bucket => {
				result[bucket.key] = {
					name: cpvs.getCPVName(bucket.key, 'EN'),
					value: bucket.doc_count
				};
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
		parse: (answer, cpvs) => {
			let result = {};
			let buckets = answer.terms_indicators.indicators_filter ? answer.terms_indicators.indicators_filter.buckets : answer.terms_indicators.buckets;
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
		parse: (answer, cpvs) => {
			let result = {};
			answer.terms_company_nuts.company_nuts_nested.buckets.sort((a, b) => {
				if (a.key < b.key) return -1;
				if (a.key > b.key) return 1;
				return 0;
			});
			answer.terms_company_nuts.company_nuts_nested.buckets.forEach(bucket => {
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
		parse: (answer, cpvs) => {
			let result = {};
			answer.terms_authority_nuts.authority_nuts_nested.buckets.sort((a, b) => {
				if (a.key < b.key) return -1;
				if (a.key > b.key) return 1;
				return 0;
			});
			answer.terms_authority_nuts.authority_nuts_nested.buckets.forEach(bucket => {
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
		parse: (answer, cpvs) => {
			let result = {};
			let buckets = answer.histogram_lots_awardDecisionDate.histogram_lots_awardDecisionDate_filter ?
				answer.histogram_lots_awardDecisionDate.histogram_lots_awardDecisionDate_filter.dates_nested.buckets :
				answer.histogram_lots_awardDecisionDate.dates_nested.buckets;
			if (buckets) {
				buckets.forEach(bucket => {
					let year = parseInt(bucket.key_as_string.slice(0, 4), 10);
					if (Utils.isValidDigiwhistYear(year)) {
						result[year] = bucket.doc_count;
					}
				});
			}
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
		parse: (answer, cpvs) => {
			let result = {};
			let buckets = answer.histogram_lots_awardDecisionDate_reverseNested.dates_filter ?
				answer.histogram_lots_awardDecisionDate_reverseNested.dates_filter.dates_nested.buckets :
				answer.histogram_lots_awardDecisionDate_reverseNested.dates_nested.buckets;
			if (buckets) {
				buckets.forEach(bucket => {
					let year = parseInt(bucket.key_as_string.slice(0, 4), 10);
					if (Utils.isValidDigiwhistYear(year)) {
						result[year] = bucket.doc_count;
					}
				});
			}
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
		parse: (answer, cpvs, tendercount) => {
			let lotsbids = answer.count_lots_bids.count_lots_bids_filter ? answer.count_lots_bids.count_lots_bids_filter.lotsbids : answer.count_lots_bids.lotsbids;
			let result = {
				bids: lotsbids.doc_count,
				bids_awarded: lotsbids.lotsbids_nested_filter.lotsbids_nested.doc_count,
				lots: answer.count_lots_bids.doc_count,
				tenders: tendercount
			};
			return result;
		}

	}
};

let clone = (o) => {
	return JSON.parse(JSON.stringify(o));
};

let buildAggregations = (aggIds) => {
	let request = {};

	let aggs = aggIds.map(id => {
		if (!Aggregations[id]) {
			console.log('invalid aggregation id', id);
		}
		return {
			id: id,
			request: clone(Aggregations[id].request),
			parse: Aggregations[id].parse
		};
	});
	aggs.forEach(agg => {
		request[agg.id] = agg.request;
	});

	let parse = (answer, cpvs, tendercount) => {
		let result = {};
		aggs.forEach(agg => {
			result[agg.id] = agg.parse(answer, cpvs, tendercount);
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

let getNestedField = fieldname => nested_objects.filter(nested => {
	return fieldname.indexOf(nested) === 0;
})[0];

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
		} else if (f.type === 'match') {
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
		} else if (f.type == 'range') {
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
	let nested = getNestedField(filter.field);
	if (nested) {
		return {
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
			'order': options.sort.ascend ? 'asc' : 'desc',
		};
		let nested = getNestedField(options.sort.field);
		if (nested) {
			body.sort[options.sort.field]['nested_path'] = nested;
		}
	}
	let aggregations = buildSearchAggregations(options);
	if (aggregations) {
		body.aggregations = aggregations;
	}
	if (options.filters && options.filters.length > 0) {
		let filters = [];
		options.filters.forEach(filter => {
			if (filter.type === 'date') {
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
	if (Object.keys(body.query).length === 0) {
		body.query.match_all = {};
	}
	return body;
};

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
				'terms': {
					'buyers.groupId': buyerIds
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
						'terms': {
							'lots.bids.bidders.groupId': bidderIds
						}
					}
				}
			}
		}
	};
};

Filters.byMainCPVdivision = (cpv) => {
	return {
		'nested': {
			'path': 'cpvs',
			'query': {
				'bool': {
					'must': [
						{'term': {'cpvs.code.main': cpv}},
						{
							'term': {
								'cpvs.isMain': true
							}
						}
					]
				}
			}
		}
	};
};

Filters.byMainCPVfull = (cpv) => {
	return {
		'nested': {
			'path': 'cpvs',
			'query': {
				'bool': {
					'must': [
						{'term': {'cpvs.code': cpv}},
						{
							'term': {
								'cpvs.isMain': true
							}
						}
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
	return {
		'bool': {
			'must': [
				query,
				{
					term: {
						'country': country_id
					}
				}
			]
		}
	};
};


module.exports.Filters = Filters;
module.exports.Aggregations = Aggregations;
module.exports.addCountryFilter = addCountryFilter;
module.exports.applyNestedFilter = applyNestedFilter;
module.exports.compactAggregations = compactAggregations;
module.exports.getNestedField = getNestedField;
module.exports.buildCountrySearchBody = buildCountrySearchBody;
module.exports.buildSearchBody = buildSearchBody;
module.exports.buildSearchAggregations = buildSearchAggregations;


module.exports.buildAggregations = buildAggregations;