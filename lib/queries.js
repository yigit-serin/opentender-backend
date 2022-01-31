const Utils = require('./utils');
const moment = require('moment');

const MAX_NUTS_AGGREGATION = 300000; // the higher, the more java heapspace is needed by elasticsearch
const MAX_CPV_AGGREGATION = 10000;

const Aggregations = {
    top_sum_finalPrice_authorities: {
        request: {
            'nested': {
                'path': 'buyers',
            },
            'aggregations': {
                'authorities_nested': {
                    'terms': {
                        'field': 'buyers.id',
                        'shard_size': 0,
                        'size': 10,
                        'order': [{ 'sum_agg>sum_finalPriceEUR': 'desc' }],
                    },
                    'aggregations': {
                        'sum_agg': {
                            'reverse_nested': {},
                            'aggregations': {
                                'sum_finalPriceEUR': {
                                    'sum': {
                                        'field': 'finalPrice.netAmountNational',
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        parse: (answer, options) => {
            let root = getAnswerAggreggation(answer, 'top_sum_finalPrice_authorities');
            let result = {
                count: root.doc_count,
                top10: [],
            };
            let buckets = root.authorities_nested.buckets;
            buckets.forEach(bucket => {
                if (bucket.sum_agg.sum_finalPriceEUR.value > 0) {
                    result.top10.push({
                        value: Utils.roundValueTwoDecimals(bucket.sum_agg.sum_finalPriceEUR.value),
                        id: bucket.key,
                    });
                }
            });
            return result;
        },
    },
    top_terms_authorities: {
        request: {
            'nested': {
                'path': 'buyers',
            },
            'aggregations': {
                'authorities_nested': {
                    'terms': {
                        'field': 'buyers.id',
                        'size': 10,
                    },
                },
            },
        },
        parse: (answer, options) => {
            let root = getAnswerAggreggation(answer, 'top_terms_authorities');
            let result = {
                count: root.doc_count,
                top10: [],
            };
            let buckets = root.authorities_nested.buckets;
            buckets.forEach(bucket => {
                result.top10.push({
                    value: bucket.doc_count,
                    id: bucket.key,
                });
            });
            return result;
        },
    },
    top_authorities: {
        request: {
            'nested': {
                'path': 'buyers',
            },
            'aggregations': {
                'authorities_nested': {
                    'terms': {
                        'field': 'buyers.id',
                        'size': 10,
                    },
                    'aggregations': {
                        'hits': {
                            'top_hits': {
                                'size': 1,
                            },
                        },
                    },
                },
            },
        },
        parse: (answer, options) => {
            let root = getAnswerAggreggation(answer, 'top_authorities');
            let result = {
                count: root.doc_count,
                top10: [],
            };
            let buckets = root.authorities_nested.buckets;
            buckets.forEach(bucket => {
                result.top10.push({
                    value: bucket.doc_count,
                    body: bucket.hits.hits.hits[0]._source,
                });
            });
            return result;
        },
    },
    top_sum_finalPrice_companies: {
        request: {
            'nested': {
                'path': 'lots.bids.bidders',
            },
            'aggregations': {
                'companies_nested': {
                    'terms': {
                        'field': 'lots.bids.bidders.id',
                        'shard_size': 0,
                        'size': 10,
                        'order': [{ 'sum_agg>sum_finalPriceEUR': 'desc' }],
                    },
                    'aggregations': {
                        'sum_agg': {
                            'reverse_nested': {},
                            'aggregations': {
                                'sum_finalPriceEUR': {
                                    'sum': {
                                        'field': 'finalPrice.netAmountNational',
                                    },
                                },
                                'total_value_of_contracts': {
                                    'sum': {
                                        field: 'totalValueOfContracts',
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        parse: (answer, options) => {
            let root = getAnswerAggreggation(answer, 'top_sum_finalPrice_companies');
            let result = {
                count: root.doc_count,
                top10: [],
            };
            let buckets = root.companies_nested.buckets;
            buckets.forEach(bucket => {
                if (bucket.sum_agg.sum_finalPriceEUR.value > 0) {
                    result.top10.push({
                        value: Utils.roundValueTwoDecimals(bucket.sum_agg.sum_finalPriceEUR.value),
                        id: bucket.key,
                    });
                }
            });
            return result;
        },
    },
    top_terms_companies: {
        request: {
            'nested': {
                'path': 'lots.bids.bidders',
            },
            'aggregations': {
                'companies_nested': {
                    'terms': {
                        'field': 'lots.bids.bidders.id',
                        'size': 10,
                    },
                },
            },
        },
        parse: (answer, options) => {
            let root = getAnswerAggreggation(answer, 'top_terms_companies');
            let result = {
                count: root.doc_count,
                top10: [],
            };
            let buckets = root.companies_nested.buckets;
            buckets.forEach(bucket => {
                result.top10.push({
                    value: bucket.doc_count,
                    id: bucket.key,
                });
            });
            return result;
        },
    },
    top_companies: {
        request: {
            'nested': {
                'path': 'lots.bids.bidders',
            },
            'aggregations': {
                'companies_nested': {
                    'terms': {
                        'field': 'lots.bids.bidders.id',
                        'size': 10,
                    },
                    'aggregations': {
                        'hits': {
                            'top_hits': {
                                'size': 1,
                            },
                        },
                    },
                },
            },
        },
        parse: (answer, options) => {
            let root = getAnswerAggreggation(answer, 'top_companies');
            let result = {
                count: root.doc_count,
                top10: [],
            };
            let buckets = root.companies_nested.buckets;
            buckets.forEach(bucket => {
                result.top10.push({
                    value: bucket.doc_count,
                    body: bucket.hits.hits.hits[0]._source,
                });
            });
            return result;
        },
    },
    top_winning_companies: {
        request: {
            'nested': {
                'path': 'lots.bids',
            },
            'aggregations': {
                'top_winning_companies_nested_filter': {
                    'filter': {
                        'query': {
                            'bool': {
                                'filter': [
                                    { 'term': { 'lots.bids.isWinning': true } },
                                ],
                            },
                        },
                    },
                    'aggregations': {
                        'companies': {
                            'nested': {
                                'path': 'lots.bids.bidders',
                            },
                            'aggregations': {
                                'companies_nested': {
                                    'terms': {
                                        'field': 'lots.bids.bidders.id',
                                        'size': 10,
                                    },
                                    'aggregations': {
                                        'hits': {
                                            'top_hits': {
                                                'size': 1,
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        parse: (answer, options) => {
            let result = {
                count: answer.top_winning_companies.doc_count,
                top10: [],
            };
            let root = getAnswerAggreggation(answer, 'top_winning_companies');
            let buckets = root.top_winning_companies_nested_filter.companies.companies_nested.buckets;
            buckets.forEach(bucket => {
                result.top10.push({
                    value: bucket.doc_count,
                    body: bucket.hits.hits.hits[0]._source,
                });
            });
            return result;
        },
    },
    sums_finalPrice: {
        request: {
            'terms': {
                'field': 'finalPrice.currency',
                'size': 100,
            },
            'aggregations': {
                'sum_price': {
                    'sum': {
                        'field': 'finalPrice.netAmount',
                    },
                },
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'sums_finalPrice');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                result[bucket.key] = Utils.roundValueTwoDecimals(bucket.sum_price.value);
            });
            return result;
        },
    },
    sum_finalPriceEUR: {
        request: {
            'sum': {
                'field': 'finalPrice.netAmountNational',
            },
        },
        parse: (answer, options) => {
            let root = getAnswerAggreggation(answer, 'sum_finalPriceEUR');
            root.value = Utils.roundValueTwoDecimals(root.value);
            return root;
        },
    },
    avg_finalPriceEUR: {
        request: {
            'avg': {
                'field': 'finalPrice.netAmountNational',
            },
        },
        parse: (answer, options) => {
            let root = getAnswerAggreggation(answer, 'avg_finalPriceEUR');
            root.value = Utils.roundValueTwoDecimals(root.value);
            return root;
        },
    },
    terms_main_cpv_divisions: {
        request: {
            'terms': {
                'field': 'ot.cpv.divisions',
                'size': MAX_CPV_AGGREGATION,
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_main_cpv_divisions');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                result[bucket.key] = {
                    name: options.library.getCPVName(bucket.key, options.lang),
                    value: bucket.doc_count,
                };
            });
            return result;
        },
    },
    terms_main_cpv_divisions_scores: {
        request: {
            'terms': {
                'field': 'ot.cpv.divisions',
                'size': MAX_CPV_AGGREGATION,
            },
            'aggregations': {
                'avg_scores': {
                    'nested': {
                        'path': 'ot.scores',
                    },
                    'aggregations': {
                        'avg_scores_nested': {
                            'terms': {
                                'field': 'ot.scores.type',
                                'size': 100,
                            },
                            'aggregations': {
                                'avg_scores_nested_avg': {
                                    'avg': {
                                        'field': 'ot.scores.value',
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_main_cpv_divisions_scores');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                let scores = {};
                let subbuckets = bucket.avg_scores.avg_scores_nested.buckets;
                subbuckets.forEach(subbucket => {
                    if (subbucket.avg_scores_nested_avg.value !== null) {
                        scores[subbucket.key] = Utils.roundValueTwoDecimals(subbucket.avg_scores_nested_avg.value);
                    }
                });
                result[bucket.key] = {
                    name: options.library.getCPVName(bucket.key, options.lang),
                    scores,
                };
            });
            return result;
        },
    },
    terms_main_cpv_categories_scores: {
        request: {
            'terms': {
                'field': 'ot.cpv.categories',
                'size': MAX_CPV_AGGREGATION,
            },
            'aggregations': {
                'avg_scores': {
                    'nested': {
                        'path': 'ot.scores',
                    },
                    'aggregations': {
                        'avg_scores_nested': {
                            'terms': {
                                'field': 'ot.scores.type',
                                'size': 100,
                            },
                            'aggregations': {
                                'avg_scores_nested_avg': {
                                    'avg': {
                                        'field': 'ot.scores.value',
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_main_cpv_categories_scores');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                let scores = {};
                let subbuckets = bucket.avg_scores.avg_scores_nested.buckets;
                subbuckets.forEach(subbucket => {
                    if (subbucket.avg_scores_nested_avg.value !== null) {
                        scores[subbucket.key] = Utils.roundValueTwoDecimals(subbucket.avg_scores_nested_avg.value);
                    }
                });
                result[bucket.key] = {
                    name: options.library.getCPVName(bucket.key, options.lang),
                    scores,
                };
            });
            return result;
        },
    },
    terms_main_cpv_groups_scores: {
        request: {
            'terms': {
                'field': 'ot.cpv.groups',
                'size': MAX_CPV_AGGREGATION,
            },
            'aggregations': {
                'avg_scores': {
                    'nested': {
                        'path': 'ot.scores',
                    },
                    'aggregations': {
                        'avg_scores_nested': {
                            'terms': {
                                'field': 'ot.scores.type',
                                'size': 100,
                            },
                            'aggregations': {
                                'avg_scores_nested_avg': {
                                    'avg': {
                                        'field': 'ot.scores.value',
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_main_cpv_groups_scores');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                let scores = {};
                let subbuckets = bucket.avg_scores.avg_scores_nested.buckets;
                subbuckets.forEach(subbucket => {
                    if (subbucket.avg_scores_nested_avg.value !== null) {
                        scores[subbucket.key] = Utils.roundValueTwoDecimals(subbucket.avg_scores_nested_avg.value);
                    }
                });
                result[bucket.key] = {
                    name: options.library.getCPVName(bucket.key, options.lang),
                    scores,
                };
            });
            return result;
        },
    },
    terms_main_cpv_full_scores: {
        request: {
            'terms': {
                'field': 'ot.cpv',
                'size': MAX_CPV_AGGREGATION,
            },
            'aggregations': {
                'avg_scores': {
                    'nested': {
                        'path': 'ot.scores',
                    },
                    'aggregations': {
                        'avg_scores_nested': {
                            'terms': {
                                'field': 'ot.scores.type',
                                'size': 100,
                            },
                            'aggregations': {
                                'avg_scores_nested_avg': {
                                    'avg': {
                                        'field': 'ot.scores.value',
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_main_cpv_full_scores');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                let scores = {};
                let subbuckets = bucket.avg_scores.avg_scores_nested.buckets;
                subbuckets.forEach(subbucket => {
                    if (subbucket.avg_scores_nested_avg.value !== null) {
                        scores[subbucket.key] = Utils.roundValueTwoDecimals(subbucket.avg_scores_nested_avg.value);
                    }
                });
                result[bucket.key] = {
                    name: options.library.getCPVName(bucket.key, options.lang),
                    scores,
                };
            });
            return result;
        },
    },
    terms_main_cpv_divisions_indicators: {
        request: {
            'terms': {
                'field': 'ot.cpv.divisions',
                'size': MAX_CPV_AGGREGATION,
            },
            'aggregations': {
                'avg_indicators_scores': {
                    'nested': {
                        'path': 'indicators',
                    },
                    'aggregations': {
                        'avg_indicators_scores_nested': {
                            'terms': {
                                'field': 'indicators.type',
                                'size': 100,
                            },
                            'aggregations': {
                                'avg_indicators_scores_nested_avg': {
                                    'avg': {
                                        'field': 'indicators.value',
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_main_cpv_divisions_indicators');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                let scores = {};
                let subbuckets = bucket.avg_indicators_scores.avg_indicators_scores_nested.buckets;
                subbuckets.forEach(subbucket => {
                    if (subbucket.avg_indicators_scores_nested_avg.value !== null) {
                        scores[subbucket.key] = Utils.roundValueTwoDecimals(subbucket.avg_indicators_scores_nested_avg.value);
                    }
                });
                result[bucket.key] = {
                    name: options.library.getCPVName(bucket.key, options.lang),
                    scores,
                };
            });
            return result;
        },
    },
    terms_main_cpv_groups: {
        request: {
            'terms': {
                'field': 'ot.cpv.groups',
                'size': MAX_CPV_AGGREGATION,
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_main_cpv_groups');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                result[bucket.key] = {
                    name: options.library.getCPVName(bucket.key, options.lang),
                    value: bucket.doc_count,
                };
            });
            return result;
        },
    },
    terms_main_cpv_categories: {
        request: {
            'terms': {
                'field': 'ot.cpv.categories',
                'size': MAX_CPV_AGGREGATION,
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_main_cpv_categories');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                result[bucket.key] = {
                    name: options.library.getCPVName(bucket.key, options.lang),
                    value: bucket.doc_count,
                };
            });
            return result;
        },
    },
    terms_main_cpv_full: {
        request: {
            'terms': {
                'field': 'ot.cpv',
                'size': MAX_CPV_AGGREGATION,
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_main_cpv_full');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                result[bucket.key] = {
                    name: options.library.getCPVName(bucket.key, options.lang),
                    value: bucket.doc_count,
                };
            });
            return result;
        },
    },
    terms_countries: {
        request: {
            terms: {
                field: 'ot.country',
                size: MAX_CPV_AGGREGATION,
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_countries');
            let buckets = root.buckets;
            buckets.forEach((bucket) => {
                result[bucket.key.toLowerCase()] = bucket.doc_count;
            });
            return result;
        },
    },
    terms_procedure_type: {
        request: {
            terms: {
                field: 'procedureType',
                size: 100,
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_procedure_type');
            let buckets = root.buckets;
            buckets.forEach((bucket) => {
                result[bucket.key] = bucket.doc_count;
            });
            return result;
        },
    },
    terms_indicators: {
        request: {
            'nested': {
                'path': 'ot.indicators',
            },
            'aggregations': {
                'terms_indicators_nested': {
                    'terms': {
                        'field': 'ot.indicators.type',
                        'size': 100,
                    },
                },
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_indicators');
            let buckets = root.terms_indicators_nested.buckets;
            buckets.forEach(bucket => {
                result[bucket.key] = bucket.doc_count;
            });
            return result;
        },
    },
    terms_indicators_score: {
        request: {
            'nested': {
                'path': 'ot.indicators',
            },
            'aggregations': {
                'terms_indicators_score_nested': {
                    'terms': {
                        'field': 'ot.indicators.type',
                        'size': 100,
                    },
                    'aggregations': {
                        'terms_indicators_score_nested_avg': {
                            'avg': {
                                'field': 'ot.indicators.value',
                            },
                        },
                    },
                },
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_indicators_score');
            let buckets = root.terms_indicators_score_nested.buckets;
            buckets.forEach(bucket => {
                if (bucket.terms_indicators_score_nested_avg.value !== null) {
                    result[bucket.key] = Utils.roundValueTwoDecimals(bucket.terms_indicators_score_nested_avg.value);
                }
            });
            return result;
        },
    },
    avg_scores: {
        request: {
            'nested': {
                'path': 'ot.scores',
            },
            'aggregations': {
                'avg_scores_nested': {
                    'terms': {
                        'field': 'ot.scores.type',
                        'size': 100,
                    },
                    'aggregations': {
                        'avg_scores_nested_avg': {
                            'avg': {
                                'field': 'ot.scores.value',
                            },
                        },
                    },
                },
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'avg_scores');
            let buckets = root.avg_scores_nested.buckets;
            buckets.forEach(bucket => {
                result[bucket.key] = Utils.roundValueTwoDecimals(bucket.avg_scores_nested_avg.value);
            });
            return result;
        },
    },
    terms_company_nuts: {
        request: {
            'nested': {
                'path': 'lots.bids.bidders',
            },
            'aggregations': {
                'company_nuts_nested': {
                    'terms': {
                        'field': 'lots.bids.bidders.address.ot.nutscode',
                        'size': MAX_NUTS_AGGREGATION,
                    },
                    'aggregations': {
                        'total_value_of_contracts': {
                            'sum': {
                                'field': 'lots.bids.bidders.totalValueOfContracts',
                            },
                        },
                    },
                },
            },
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
                result[nut] = {
                    count: ((result[nut] && result[nut].count) || 0) + bucket.doc_count,
                    value: ((result[nut] && result[nut].value) || 0) + bucket.total_value_of_contracts.value,
                };
            });
            return result;
        },
    },
    terms_authority_nuts: {
        request: {
            'nested': {
                'path': 'buyers',
            },
            'aggregations': {
	    	'authority_nuts3_nested': {
		  'terms': {
		    'field': 'buyers.address.ot.nutscode.nuts3',
		    'size': MAX_NUTS_AGGREGATION,
		  },
		  'aggregations': {
		    'total_value_of_contracts': {
		      'sum': {
			'field': 'buyers.totalValueOfContracts',
		      },
		    },
		  },
		},
                'authority_nuts2_nested': {
                    'terms': {
                        'field': 'buyers.address.ot.nutscode.nuts2',
                        'size': MAX_NUTS_AGGREGATION,
                    },
                    'aggregations': {
                        'total_value_of_contracts': {
                            'sum': {
                                'field': 'buyers.totalValueOfContracts',
                            },
                        },
                    },
                },
                'authority_nuts1_nested': {
                    'terms': {
                        'field': 'buyers.address.ot.nutscode.nuts1',
                        'size': MAX_NUTS_AGGREGATION,
                    },
                    'aggregations': {
                        'total_value_of_contracts': {
                            'sum': {
                                'field': 'buyers.totalValueOfContracts',
                            },
                        },
                    },
                },
	    	'authority_nuts0_nested': {
		  'terms': {
		    'field': 'buyers.address.ot.nutscode.nuts0',
		    'size': MAX_NUTS_AGGREGATION,
		  },
		  'aggregations': {
		    'total_value_of_contracts': {
		      'sum': {
			'field': 'buyers.totalValueOfContracts',
		      },
		    },
		  },
		},
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_authority_nuts');
	    let buckets = root.authority_nuts1_nested.buckets.concat(root.authority_nuts2_nested.buckets).concat(root.authority_nuts3_nested.buckets).concat(root.authority_nuts0_nested.buckets);
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
                if (bucket.total_value_of_contracts) {
                    result[nut] = {
                        count: ((result[nut] && result[nut].count) || 0) + bucket.doc_count,
                        value: ((result[nut] && result[nut].value) || 0) + bucket.total_value_of_contracts.value,
                    };
                } else {
                    result[nut] = (result[nut] || 0) + bucket.doc_count;
                }
            });
            return result;
        },
    },
    histogram: {
        request: {
            'date_histogram': { 'field': 'ot.date', 'interval': 'year' },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'histogram');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                let year = parseInt(bucket.key_as_string.slice(0, 4), 10);
                result[year] = bucket.doc_count;
            });
            return result;
        },
    },
    histogram_indicators: {
        request: {
            'date_histogram': { 'field': 'ot.date', 'interval': 'year' },
            'aggregations': {
                'avg_indicators_scores': {
                    'nested': {
                        'path': 'ot.indicators',
                    },
                    'aggregations': {
                        'avg_indicators_scores_nested': {
                            'terms': {
                                'field': 'ot.indicators.type',
                                'size': 100,
                            },
                            'aggregations': {
                                'avg_indicators_scores_nested_avg': {
                                    'avg': {
                                        'field': 'ot.indicators.value',
                                    },
                                },
                            },
                        },
                    },
                },
                'avg_scores': {
                    'nested': {
                        'path': 'ot.scores',
                    },
                    'aggregations': {
                        'avg_scores_nested': {
                            'terms': {
                                'field': 'ot.scores.type',
                                'size': 100,
                            },
                            'aggregations': {
                                'avg_scores_nested_avg': {
                                    'avg': {
                                        'field': 'ot.scores.value',
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'histogram_indicators');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                let year = parseInt(bucket.key_as_string.slice(0, 4), 10);
                let subbuckets = bucket.avg_indicators_scores.avg_indicators_scores_nested.buckets;
                subbuckets.forEach(subbucket => {
                    if (subbucket.avg_indicators_scores_nested_avg.value !== null) {
                        result[subbucket.key] = result[subbucket.key] || {};
                        result[subbucket.key][year] = Utils.roundValueTwoDecimals(subbucket.avg_indicators_scores_nested_avg.value);
                    }
                });
                subbuckets = bucket.avg_scores.avg_scores_nested.buckets;
                subbuckets.forEach(subbucket => {
                    if (subbucket.avg_scores_nested_avg.value !== null) {
                        result[subbucket.key] = result[subbucket.key] || {};
                        result[subbucket.key][year] = Utils.roundValueTwoDecimals(subbucket.avg_scores_nested_avg.value);
                    }
                });
            });
            return result;
        },
    },
    histogram_finalPriceEUR: {
        request: {
            'date_histogram': { 'field': 'ot.date', 'interval': 'year' },
            'aggregations': {
                'sum_finalPriceEUR': {
                    'sum': {
                        'field': 'finalPrice.netAmountNational',
                    },
                },
                'avg_finalPriceEUR': {
                    'avg': {
                        'field': 'finalPrice.netAmountNational',
                    },
                },
                'total_value_of_contracts': {
                    'sum': {
                        field: 'totalValueOfContracts',
                    },
                },
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'histogram_finalPriceEUR');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                let year = parseInt(bucket.key_as_string.slice(0, 4), 10);
                bucket.sum_finalPriceEUR.value = Utils.roundValueTwoDecimals(bucket.sum_finalPriceEUR.value / 100);
                bucket.avg_finalPriceEUR.value = Utils.roundValueTwoDecimals(bucket.avg_finalPriceEUR.value / 100);
                result[year] = {
                    value: bucket.doc_count,
                    sum_finalPriceEUR: bucket.sum_finalPriceEUR,
                    avg_finalPriceEUR: bucket.avg_finalPriceEUR,
                };
            });
            return result;
        },
    },
    histogram_percentile_indicators: {
        request: {
            'date_histogram': { 'field': 'ot.date', 'interval': 'year' },
            'aggregations': {
                'indicators_percentiles': {
                    'nested': {
                        'path': 'indicators',
                    },
                    'aggregations': {
                        'indicators_percentiles_nested': {
                            'terms': {
                                'field': 'indicators.type',
                                'size': 100,
                            },
                            'aggregations': {
                                'percentiles_indicators_percentiles': {
                                    'percentiles': {
                                        'field': 'indicators.value',
                                        'percents': [25, 50, 75, 100],
                                    },
                                },
                            },
                        },
                    },
                },
                'scores_percentiles': {
                    'nested': {
                        'path': 'ot.scores',
                    },
                    'aggregations': {
                        'scores_percentiles_nested': {
                            'terms': {
                                'field': 'ot.scores.type',
                                'size': 100,
                            },
                            'aggregations': {
                                'percentiles_scores_percentiles': {
                                    'percentiles': {
                                        'field': 'ot.scores.value',
                                        'percents': [25, 50, 75, 100],
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        parse: (answer, options) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'histogram_percentile_indicators');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                let year = parseInt(bucket.key_as_string.slice(0, 4), 10);
                let subbuckets = bucket.indicators_percentiles.indicators_percentiles_nested.buckets;
                subbuckets.forEach(subbucket => {
                    result[subbucket.key] = result[subbucket.key] || {};
                    let vals = subbucket.percentiles_indicators_percentiles.values;
                    Object.keys(vals).forEach(key => {
                        if (isNaN(vals[key])) {
                            vals[key] = null;
                        }
                    });
                    result[subbucket.key][year] = vals;
                });

                subbuckets = bucket.scores_percentiles.scores_percentiles_nested.buckets;
                subbuckets.forEach(subbucket => {
                    result[subbucket.key] = result[subbucket.key] || {};
                    let vals = subbucket.percentiles_scores_percentiles.values;
                    Object.keys(vals).forEach(key => {
                        if (isNaN(vals[key])) {
                            vals[key] = null;
                        }
                    });
                    result[subbucket.key][year] = vals;
                });
            });
            return result;
        },
    },
    histogram_distribution_indicators: {
        request: {
            'date_histogram': { 'field': 'ot.date', 'interval': 'year' },
            'aggregations': {
                'indicators_distribution': {
                    'nested': {
                        'path': 'ot.indicators',
                    },
                    'aggregations': {
                        'indicators_distribution_nested': {
                            'terms': {
                                'field': 'ot.indicators.type',
                                'size': 100,
                            },
                            'aggregations': {
                                'indicators_distribution_histogram': {
                                    'histogram': {
                                        'field': 'ot.indicators.value',
                                        'interval': 5,
                                    },
                                },
                            },
                        },
                    },
                },
                'scores_distribution': {
                    'nested': {
                        'path': 'ot.scores',
                    },
                    'aggregations': {
                        'scores_distribution_nested': {
                            'terms': {
                                'field': 'ot.scores.type',
                                'size': 100,
                            },
                            'aggregations': {
                                'scores_distribution_histogram': {
                                    'histogram': {
                                        'field': 'ot.scores.value',
                                        'interval': 5,
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        parse: (answer, options) => {
            let result = {};

            let root = getAnswerAggreggation(answer, 'histogram_distribution_indicators');
            // console.log(JSON.stringify(root, null, 4));
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                let year = parseInt(bucket.key_as_string.slice(0, 4), 10);
                let subbuckets = bucket.indicators_distribution.indicators_distribution_nested.buckets;
                subbuckets.forEach(subbucket => {
                    result[subbucket.key] = result[subbucket.key] || {};
                    let vals = subbucket.indicators_distribution_histogram.buckets;
                    let distribution = {};
                    vals.forEach(val => {
                        distribution[val.key] = val.doc_count;
                    });
                    result[subbucket.key][year] = distribution;
                });
                subbuckets = bucket.scores_distribution.scores_distribution_nested.buckets;
                subbuckets.forEach(subbucket => {
                    result[subbucket.key] = result[subbucket.key] || {};
                    let vals = subbucket.scores_distribution_histogram.buckets;
                    let distribution = {};
                    vals.forEach(val => {
                        distribution[val.key] = val.doc_count;
                    });
                    result[subbucket.key][year] = distribution;
                });
            });
            return result;
        },
    },
};

const ParameterAggregations = {
    avg_score_para: {
        request: (params) => {
            console.log(params.name);
            let req = {
                'avg': {
                    'field': 'ot.score.' + params.name,
                },
            };
            return req;
        },
        parse: (answer, options, params) => {
            let root = getAnswerAggreggation(answer, 'avg_score_para');
            let result = {};
            if (root.value !== null) {
                result[params.name] = Utils.roundValueTwoDecimals(root.value);
            }
            return result;
        },
    },
    avg_score_custom_para: {
        request: (params) => {
            let fields = Object.keys(params.custom).map(key => 'ot.indicator.' + key);
            let weights = Object.values(params.custom);
            // far too slow and not working
            // if (params.script === 'groovy') {
            // 	return {
            // 		"scripted_metric": {
            // 			"init_script": `_agg['sum'] = 0; _agg['count'] = 0`,
            // 			"map_script":
            // 			`       sum = 0;
            // 				sum_weights = 0;
            // 				weights = ` + JSON.stringify(weights) + `;
            // 				names = ` + JSON.stringify(fields) + `;
            // 				for (i = 0; i < ` + fields.length + `; i++) {
            // 					weight = weights[i];
            // 					indicator = doc[names[i]]; //TODO get doc child object
            // 					if (!indicator.empty) {
            // 						sum += indicator.value * weight;
            // 						sum_weights += weight;
            // 					}
            // 				}
            // 				if (sum_weights > 0) {
            // 					_agg['sum'] += sum/sum_weights;
            // 					_agg['count'] += 1;
            // 				}
            // 			`,
            // 			"reduce_script":
            // 				`
            // 				sum = 0;
            // 				count = 0;
            // 				for (a in _aggs) {
            // 					sum += a.sum;
            // 					count += a.count;
            // 				}
            // 				return count>0?(sum/count):null;
            // 			`
            // 		}
            // 	}
            // } else {
            return {
                'scripted_metric': {
                    'params': { 'fields': fields, 'weights': weights },
                    'init_script': {
                        'lang': 'native',
                        'script': 'weighted_avg_init',
                    },
                    'map_script': {
                        'lang': 'native',
                        'script': 'weighted_avg_map',
                    },
                    'combine_script': {
                        'lang': 'native',
                        'script': 'weighted_avg_combine',
                    },
                    'reduce_script': {
                        'lang': 'native',
                        'script': 'weighted_avg_reduce',
                    },
                },
            };
            // }
        },
        parse: (answer, options, params) => {
            let root = getAnswerAggreggation(answer, 'avg_score_custom_para');
            let result = {};
            if (root.value !== null) {
                result[params.name] = Utils.roundValueTwoDecimals(root.value);
            }
            return result;
        },
    },
    avg_indicator_para: {
        request: (params) => {
            let req = {
                'avg': {
                    'field': 'ot.indicator.' + params.name,
                },
            };
            return req;
        },
        parse: (answer, options, params) => {
            let root = getAnswerAggreggation(answer, 'avg_indicator_para');
            let result = {};
            if (root.value !== null) {
                result[params.name] = Utils.roundValueTwoDecimals(root.value);
            }
            return result;
        },
    },
    terms_indicators_score_para: {
        request: (params) => {
            let req = {
                'nested': {
                    'path': 'ot.indicators',
                },
                'aggregations': {
                    'terms_indicators_score_nested': {
                        'filter': {
                            'query': {
                                'bool': {
                                    'filter': [
                                        { 'match_phrase_prefix': { 'ot.indicators.type': params.name } },
                                    ],
                                },
                            },
                        },
                        'aggregations': {
                            'terms_indicators_score_filtered': {
                                'terms': {
                                    'field': 'ot.indicators.type',
                                    'size': 100,
                                },
                                'aggregations': {
                                    'terms_indicators_score_nested_avg': {
                                        'avg': {
                                            'field': 'ot.indicators.value',
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            };
            return req;
        },
        parse: (answer, options, params) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_indicators_score_para');
            let buckets = root.terms_indicators_score_nested.terms_indicators_score_filtered.buckets;
            buckets.forEach(bucket => {
                if (bucket.terms_indicators_score_nested_avg.value !== null) {
                    result[bucket.key] = Utils.roundValueTwoDecimals(bucket.terms_indicators_score_nested_avg.value);
                }
            });
            return result;
        },
    },
    histogram_indicators_para: {
        request: (params) => {
            let req = {
                'date_histogram': { 'field': 'ot.date', 'interval': 'year' },
                'aggregations': {
                    'avg_indicators_scores': {
                        'nested': {
                            'path': 'ot.indicators',
                        },
                        'aggregations': {
                            'avg_indicators_scores_nested': {
                                'filter': {
                                    'query': {
                                        'bool': {
                                            'filter': [
                                                { 'match_phrase_prefix': { 'ot.indicators.type': params.name } },
                                            ],
                                        },
                                    },
                                },
                                'aggregations': {
                                    'avg_indicators_scores_nested_filtered': {
                                        'terms': {
                                            'field': 'ot.indicators.type',
                                            'size': 100,
                                        },
                                        'aggregations': {
                                            'avg_indicators_scores_nested_avg': {
                                                'avg': {
                                                    'field': 'ot.indicators.value',
                                                },
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                    'avg_scores': {
                        'avg': {
                            'field': 'ot.score.' + params.name,
                        },
                    },
                },
            };
            return req;
        },
        parse: (answer, options, params) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'histogram_indicators_para');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                let year = parseInt(bucket.key_as_string.slice(0, 4), 10);
                let subbuckets = bucket.avg_indicators_scores.avg_indicators_scores_nested.avg_indicators_scores_nested_filtered.buckets;
                subbuckets.forEach(subbucket => {
                    if (subbucket.avg_indicators_scores_nested_avg.value !== null) {
                        result[subbucket.key] = result[subbucket.key] || {};
                        result[subbucket.key][year] = Utils.roundValueTwoDecimals(subbucket.avg_indicators_scores_nested_avg.value);
                    }
                });
                if (bucket.avg_scores.value !== null) {
                    result[params.name] = result[params.name] || {};
                    result[params.name][year] = Utils.roundValueTwoDecimals(bucket.avg_scores.value);
                }
            });
            return result;
        },
    },
    histogram_indicators_custom_para: {
        request: (params) => {
            let req = {
                'date_histogram': { 'field': 'ot.date', 'interval': 'year' },
                'aggregations': {
                    'avg_indicators_scores': {
                        'nested': {
                            'path': 'indicators',
                        },
                        'aggregations': {
                            'avg_indicators_scores_nested': {
                                'filter': {
                                    'query': {
                                        'bool': {
                                            'filter': [
                                                { 'match_phrase_prefix': { 'indicators.type': params.name } },
                                            ],
                                        },
                                    },
                                },
                                'aggregations': {
                                    'avg_indicators_scores_nested_filtered': {
                                        'terms': {
                                            'field': 'indicators.type',
                                            'size': 100,
                                        },
                                        'aggregations': {
                                            'avg_indicators_scores_nested_avg': {
                                                'avg': {
                                                    'field': 'indicators.value',
                                                },
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                    'avg_score_custom_para': ParameterAggregations.avg_score_custom_para.request(params),
                },
            };
            return req;
        },
        parse: (answer, options, params) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'histogram_indicators_custom_para');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                let year = parseInt(bucket.key_as_string.slice(0, 4), 10);
                let subbuckets = bucket.avg_indicators_scores.avg_indicators_scores_nested.avg_indicators_scores_nested_filtered.buckets;
                subbuckets.forEach(subbucket => {
                    if (subbucket.avg_indicators_scores_nested_avg.value !== null) {
                        result[subbucket.key] = result[subbucket.key] || {};
                        result[subbucket.key][year] = Utils.roundValueTwoDecimals(subbucket.avg_indicators_scores_nested_avg.value);
                    }
                });
                let score = ParameterAggregations.avg_score_custom_para.parse(bucket, options, params);
                if (score[params.name] !== null) {
                    result[params.name] = result[params.name] || {};
                    result[params.name][year] = score[params.name];
                }
            });
            return result;
        },
    },
    histogram_indicator_para: {
        request: (params) => {
            let req = {
                'date_histogram': { 'field': 'ot.date', 'interval': 'year' },
                'aggregations': {
                    'avg_indicator_scores': {
                        'nested': {
                            'path': 'indicators',
                        },
                        'aggregations': {
                            'avg_indicator_scores_nested': {
                                'filter': {
                                    'query': {
                                        'bool': {
                                            'filter': [
                                                { 'term': { 'indicators.type': params.name } },
                                            ],
                                        },
                                    },
                                },
                                'aggregations': {
                                    'avg_indicator_scores_nested_value': {
                                        'avg': {
                                            'field': 'indicators.value',
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            };
            return req;
        },
        parse: (answer, options, params) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'histogram_indicator_para');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                let year = parseInt(bucket.key_as_string.slice(0, 4), 10);
                if (bucket.avg_indicator_scores.avg_indicator_scores_nested.avg_indicator_scores_nested_value.value !== null) {
                    result[params.name] = result[params.name] || {};
                    result[params.name][year] = Utils.roundValueTwoDecimals(bucket.avg_indicator_scores.avg_indicator_scores_nested.avg_indicator_scores_nested_value.value);
                }
            });
            return result;
        },
    },
    terms_main_cpv_divisions_score_para: {
        request: (params) => {
            let req = {
                'terms': {
                    'field': 'ot.cpv.divisions',
                    'size': MAX_CPV_AGGREGATION,
                },
                'aggregations': {
                    'avg_score': {
                        'avg': {
                            'field': 'ot.score.' + params.name,
                        },
                    },
                },
            };
            return req;
        },
        parse: (answer, options, params) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_main_cpv_divisions_score_para');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                if (bucket.avg_score.value !== null) {
                    result[bucket.key] = {
                        name: options.library.getCPVName(bucket.key, options.lang),
                        value: Utils.roundValueTwoDecimals(bucket.avg_score.value),
                    };
                }
            });
            return result;
        },
    },
    terms_main_cpv_divisions_score_custom_para: {
        request: (params) => {
            let req = {
                'terms': {
                    'field': 'ot.cpv.divisions',
                    'size': MAX_CPV_AGGREGATION,
                },
                'aggregations': {
                    'avg_score_custom_para': ParameterAggregations.avg_score_custom_para.request(params),
                },
            };
            return req;
        },
        parse: (answer, options, params) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_main_cpv_divisions_score_custom_para');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                let score = ParameterAggregations.avg_score_custom_para.parse(bucket, options, params);
                if (score[params.name] !== null) {
                    result[bucket.key] = {
                        name: options.library.getCPVName(bucket.key, options.lang),
                        value: Utils.roundValueTwoDecimals(score[params.name]),
                    };
                }
            });
            return result;
        },
    },
    terms_main_cpv_divisions_indicator_para: {
        request: (params) => {
            let req = {
                'terms': {
                    'field': 'ot.cpv.divisions',
                    'size': MAX_CPV_AGGREGATION,
                },
                'aggregations': {
                    'avg_indicator': {
                        'nested': {
                            'path': 'indicators',
                        },
                        'aggregations': {
                            'avg_indicator_nested': {
                                'filter': {
                                    'query': {
                                        'bool': {
                                            'filter': [
                                                { 'term': { 'indicators.type': params.name } },
                                            ],
                                        },
                                    },
                                },
                                'aggregations': {
                                    'avg_indicator_nested_value': {
                                        'avg': {
                                            'field': 'indicators.value',
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            };
            return req;
        },
        parse: (answer, options, params) => {
            let result = {};
            let root = getAnswerAggreggation(answer, 'terms_main_cpv_divisions_indicator_para');
            let buckets = root.buckets;
            buckets.forEach(bucket => {
                if (bucket.avg_indicator.avg_indicator_nested.avg_indicator_nested_value.value !== null) {
                    result[bucket.key] = {
                        name: options.library.getCPVName(bucket.key, options.lang),
                        value: Utils.roundValueTwoDecimals(bucket.avg_indicator.avg_indicator_nested.avg_indicator_nested_value.value),
                    };
                }
            });
            return result;
        },
    },
};

const Filters = {
    all: () => {
        return {
            match_all: {},
        };
    },
    byBuyers: (buyerIds) => {
        return {
            'nested': {
                'path': 'buyers',
                'query': {
                    'bool': {
                        'filter': [
                            { 'terms': { 'buyers.id': buyerIds } },
                        ],
                    },
                },
            },
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
                                    { 'terms': { 'lots.bids.bidders.id': bidderIds } },
                                ],
                            },
                        },
                    },
                },
            },
        };
    },
    byAuthorityNuts: (nutscode, level) => {
        let term = {};
        let field = 'buyers.address.ot.nutscode';
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
                            { 'term': term },
                        ],
                    },
                },
            },
        };
    },
    byMainCPV: (cpv, level) => {
        let term = {};
        term['ot.cpv' + (level ? '.' + level : '')] = cpv;
        return { 'term': term };
    },
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
                    filter: { query: { bool: { filter: nested_filters } } },
                    aggregations: agg.aggregations,
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

let parseSearchAggregations = (aggregations, options, filteredAggregations = {}) => {
    let resolveNode = n => {
        if (!n) {
            return;
        }
        delete n.doc_count_error_upper_bound;
        delete n.sum_other_doc_count;
        let list = Object.keys(n);
        list.forEach(key => {
            if (key[0] !== '_') {
                return;
            }
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
            if (key.indexOf('_cpvs_code') === 0) {
                o.buckets.forEach(bucket => {
                    bucket.name = options.library.getCPVName(bucket.key, options.lang);
                });
            } else if (key.indexOf('_lots_awardDecisionDate') === 0) {
                o.buckets = o.buckets.filter(bucket => {
                    return bucket.doc_count > 0;
                }).map(bucket => {
                    return {
                        key: parseInt(bucket.key_as_string.slice(0, 4), 10),
                        doc_count: bucket.doc_count,
                    };
                }).filter(bucket => {
                    return Utils.isValidDigiwhistYear(bucket.key);
                });
            } else if (key.indexOf('_body_sector_cpvCodes') === 0) {
                o.buckets.forEach((bucket, index) => {
                    o.buckets[index].label = options.library.getCPVName(bucket.key);
                });
            } else if (key.indexOf('_lots_committeeApprovalDate') === 0) {
                o.buckets = o.buckets.filter(bucket => {
                    return bucket.doc_count > 0;
                }).map(bucket => {
                    return {
                        key: parseInt(bucket.key_as_string.slice(0, 4), 10),
                        doc_count: bucket.doc_count,
                    };
                }).filter(bucket => {
                    return Utils.isValidDigiwhistYear(bucket.key);
                });
            } else if (key.indexOf('_range') !== -1) {
                n[key] = {
                    min: o.min,
                    max: o.max,
                };
            } else if (
              (key.indexOf('_estimatedStartDate') === 0) ||
              (key.indexOf('_estimatedCompletionDate') === 0)
            ) {
                o.buckets = o.buckets.filter(bucket => {
                    return bucket.doc_count > 0;
                }).map(bucket => {
                    return {
                        key: parseInt(bucket.key_as_string.slice(0, 4), 10),
                        doc_count: bucket.doc_count,
                    };
                }).filter(bucket => {
                    return Utils.isValidCentury(bucket.key);
                });
            }
        });
    };

    // Object.keys(aggregations).forEach((aggregationKey) => {
    // 	if (aggregations[aggregationKey].buckets) {
    // 		aggregations[aggregationKey].buckets.forEach((bucket, index) => {
    // 			if (bucket.doc_count) {
    // 				aggregations[aggregationKey].buckets[index].doc_count = 0;
    // 			}
    // 			const filteredBucket = filteredAggregations[aggregationKey].buckets.find(({key}) => key === bucket.key);
    // 			if (filteredBucket) {
    // 				aggregations[aggregationKey].buckets[index].doc_count = filteredBucket.doc_count || 0;
    // 			}
    // 		});
    // 	}
    // });

    resolveNode(aggregations);
};

let buildSearchFilter = (filter) => {
    if (!filter) {
        return null;
    }

    let buildQuery = (f, mode, parseValue) => {
        let values = Array.isArray(f.value) ? f.value : [f.value];
        if (values.length > 0) {
            let b = { or: [] };
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
        } else if (f.type === 'bool') {
            return buildQuery(f, 'term', (v) => {
                return v;
            });
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
                    return (isNaN(value)) ? null : { lt: value };
                });
            } else if (f.mode === '<=') {
                return buildQuery(f, 'range', (v) => {
                    let value = parseFloat(v);
                    return (isNaN(value)) ? null : { lte: value };
                });
            } else if (f.mode === '>') {
                return buildQuery(f, 'range', (v) => {
                    let value = parseFloat(v);
                    return (isNaN(value)) ? null : { gt: value };
                });
            } else if (f.mode === '>=') {
                return buildQuery(f, 'range', (v) => {
                    let value = parseFloat(v);
                    return (isNaN(value)) ? null : { gte: value };
                });
            }
            return null;
        } else if (f.type === 'date') {
            if (f.mode === '=') {
                return buildQuery(f, 'range', (v) => {
                    let value = moment(v);
                    return value.isValid ? { gte: value.toDate(), lt: value.add(1, 'd').toDate() } : null;
                });
            } else if (f.mode === '>') {
                return buildQuery(f, 'range', (v) => {
                    let value = moment(v);
                    return value.isValid ? { gt: value.toDate() } : null;
                });
            } else if (f.mode === '>=') {
                return buildQuery(f, 'range', (v) => {
                    let value = moment(v);
                    return value.isValid ? { gte: value.toDate() } : null;
                });
            } else if (f.mode === '<') {
                return buildQuery(f, 'range', (v) => {
                    let value = moment(v);
                    return value.isValid ? { lt: value.toDate() } : null;
                });
            } else if (f.mode === '<=') {
                return buildQuery(f, 'range', (v) => {
                    let value = moment(v);
                    return value.isValid ? { lte: value.toDate() } : null;
                });
            }
            return null;
        } else if (f.type === 'years') {
            let m = { range: {} };
            m.range[f.field] = {
                gte: new Date(f.value[0], 0, 1).valueOf(),
                lte: new Date(f.value[1], 0, 1).valueOf(),
            };
            return m;
        } else if (f.type === 'weighted') {
            if (f.weights) {
                let fields = Object.keys(f.weights).map(key => 'ot.indicator.' + key);
                let weights = Object.values(f.weights);
                let m = {
                    'script': {
                        'params': {
                            gte: f.value[0],
                            lte: f.value[1],
                            fields: fields,
                            weights: weights,
                        },
                        'lang': 'native',
                        'script': 'weighted_avg_range',
                    },
                };
                return m;
            }
            return null;
        } else if (f.type === 'range') {
            let m = { range: {} };
            m.range[f.field] = {
                gte: f.value[0],
                lte: f.value[1],
            };
            if (f.subrequest) {
                return {
                    'bool': {
                        'filter': [
                            { 'term': f.subrequest },
                            m,
                        ],
                    },
                };
            }
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
                'filter': subfilters,
            },
        };
    }
    if (nested) {
        return {
            'nested': {
                'path': nested,
                'query': query,
            },
        };
    }
    return query;
};

let buildSearchBody = (options, disableDefaultSort, allowSearchAggregations) => {
    let body = {
        query: {
            match_all: {},
        },
    };
    if (!disableDefaultSort) {
        body.sort = {
            'modified': {
                'order': 'desc',
            },
        };
    }

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
    if (allowSearchAggregations) {
        body.aggregations = buildSearchAggregations(options) || undefined;
    }
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
                    'filter': filters,
                },
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
                    { 'term': term },
                ],
            },
        };
    } else if (body.query.bool && body.query.bool.filter) {
        body.query.bool.filter.push({ 'term': term });
    } else {
        console.log('unknown search body format', body);
    }
    return body;
};

let buildCountrySearchBody = (options, country_id, allowSearchAggregations) => {
    let body = buildSearchBody(options, false, allowSearchAggregations);
    if (country_id) {
        body = addCountrySearchBody(body, { 'ot.country': country_id });
    }
    return body;
};

let buildSearchAggregations = (options) => {
    let result = null;
    let resolveAgg = (agg, node) => {
        if (!agg.field) {
            return;
        }

        let name = '_' + agg.field.replace(/\./g, '_');
        let nested = Utils.getNestedField(agg.field);
        if (nested) {
            let aa = {
                'nested': {
                    'path': nested,
                },
                aggregations: {},
            };
            if (agg.type === 'years') {
                aa.aggregations[name + '_nested'] = {
                    'date_histogram': { 'field': agg.field, 'interval': 'year' },
                };
            } else if (agg.type === 'range') {
                aa.aggregations[name + '_nested'] = {
                    'stats': { 'field': agg.field },
                };
            } else {
                aa.aggregations[name + '_nested'] = {
                    'terms': { 'field': agg.field, size: agg.size || undefined },
                };
            }
            node[name] = aa;
            return;
        }

        if (agg.type === 'sum') {
            node[name + '_sum'] = { 'sum': { 'field': agg.field } };
            return;
        } else if (agg.type === 'top') {
            node[name + '_hits'] = { 'top_hits': { 'size': 1, _source: { include: [agg.field] } } };
            return;
        } else if (agg.type === 'histogram') {
            node[name + '_over_time'] = { 'date_histogram': { 'field': agg.field, 'interval': 'year' } };
            return;
        } else if (agg.type === 'range') {
            node[name + '_range'] = { 'stats': { 'field': agg.field } };
            return;
        } else if (agg.type === 'value') {
            return;
        }
        let aa = { 'terms': { 'field': agg.field, size: agg.size || undefined } };
        if (agg.aggregations) {
            aa.aggregations = {};
            agg.aggregations.forEach(aagg => {
                resolveAgg(aagg, aa.aggregations);
            });
        }
        node[name] = aa;
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

let buildAggregations = (aggConfigs) => {
    let request = {};

    let aggs = aggConfigs.map(aggConfig => {
        if (typeof aggConfig === 'string') {
            if (!Aggregations[aggConfig]) {
                console.log('invalid aggregation id', aggConfig);
            }
            return {
                id: aggConfig,
                request: Utils.clone(Aggregations[aggConfig].request),
                parse: Aggregations[aggConfig].parse,
            };
        } else {
            if (!ParameterAggregations[aggConfig.id]) {
                console.log('invalid parameterized aggregation id', aggConfig.id);
            }
            return {
                id: aggConfig.id,
                request: ParameterAggregations[aggConfig.id].request(aggConfig),
                parse: ParameterAggregations[aggConfig.id].parse,
            };
        }
    });
    aggs.forEach(agg => {
        request[agg.id] = agg.request;
    });

    let parse = (answer, options) => {
        let result = {};
        aggs.forEach(agg => {
            result[agg.id] = agg.parse(answer, options, aggConfigs.find(c => (typeof c === 'object' && c.id === agg.id)));
        });
        return result;
    };

    return {
        request, parse,
    };
};

let addCountryFilter = (query, country_id) => {
    if (!country_id) {
        return query;
    }
    if (query.bool && query.filter) {
        query.filter.push({ 'term': { 'ot.country': country_id } });
        return query;
    }
    return {
        'bool': {
            'filter': [
                query,
                { 'term': { 'ot.country': country_id } },
            ],
        },
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
module.exports.MAX_CPV_AGGREGATION = MAX_CPV_AGGREGATION;
module.exports.MAX_NUTS_AGGREGATION = MAX_NUTS_AGGREGATION;
