const zlib = require('zlib');
const Store = require('./store.js');
const Library = require('./library.js');
const Queries = require('./queries.js');
const Utils = require('./utils.js');
const CSV = require('./csv.js');

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
            let stats = aggs.parse(result.aggregations, { library: this.library, lang: lang });
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
        let subindicator = null;

        let indicatorFilter = (options.filters || []).find(f => f.field === 'ot.scores.type' || f.field === 'indicators.type' || f.field === 'ot.indicators.type' || (f.type === 'weighted'));
        if (indicatorFilter) {
            if (indicatorFilter.type !== 'weighted') {
                indicator = indicatorFilter && indicatorFilter.value ? indicatorFilter.value[0] : '';
                isSubindicator = indicatorFilter && (indicatorFilter.field === 'indicators.type' || indicatorFilter.field === 'ot.indicators.type');
            } else {
                indicator = indicatorFilter && indicatorFilter.field;
            }
        }

        let subindicatorFilter = (options.filters || []).find(f => f.field === 'ot.indicators.type');
        if (subindicatorFilter) {
            subindicator = subindicatorFilter && subindicatorFilter.value ? subindicatorFilter.value[0] : '';
        }
        let b = Queries.buildCountrySearchBody(options, country_id, false);

        let aggsConfig = [
            'histogram',
            'terms_main_cpv_divisions',
            { id: 'terms_indicators_score_para', name: indicator },
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
                aggsConfig.push({ id: 'avg_score_custom_para', name: indicator, custom: customWeights });
                aggsConfig.push({ id: 'histogram_indicators_custom_para', name: indicator, custom: customWeights });
                aggsConfig.push({
                    id: 'terms_main_cpv_divisions_score_custom_para',
                    name: indicator,
                    custom: customWeights,
                });
            } else {
                aggsConfig.push({ id: 'avg_score_para', name: indicator });
                aggsConfig.push({ id: 'histogram_indicators_para', name: indicator });
                aggsConfig.push({ id: 'terms_main_cpv_divisions_score_para', name: indicator });
            }
        } else {
            aggsConfig.push({ id: 'avg_indicator_para', name: subindicator || indicator });
            aggsConfig.push({ id: 'histogram_indicator_para', name: indicator });
            aggsConfig.push({ id: 'terms_main_cpv_divisions_indicator_para', name: indicator });
        }

        console.log('Z-PARAMS', aggsConfig);
        let aggs = Queries.buildAggregations(aggsConfig);
        b.aggregations = aggs.request;
        console.log(JSON.stringify(b));
        this.store.Tender.search(b, 0, 0, (err, result) => {
            if (err) {
                console.error(err);
                return cb(err);
            }
            let data = aggs.parse(result.aggregations, { library: this.library, lang: options.lang });
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
        let aggs = Queries.buildAggregations([
            'terms_main_cpv_divisions',
            'top_terms_companies',
            'top_terms_authorities',
            'top_sum_finalPrice_companies',
            'top_sum_finalPrice_authorities',
            'terms_authority_nuts',
            'histogram_finalPriceEUR',
        ]);

        let b = Queries.buildCountrySearchBody(options, country_id, false);

        b.aggregations = aggs.request;

        if (b.query.bool && b.query.bool.filter) {
            Queries.applyNestedFilterToAggregations(b.aggregations, b.query.bool.filter);
        }

        this.store.Tender.search(b, 0, 0, (err, result) => {
            if (err) {
                console.error(err);
                return cb(err);
            }
            let stats = aggs.parse(result.aggregations, { library: this.library, lang: options.lang });

            // now get the some aggregations again without indicator filter, so averages can be calculated
            if (b.query.bool && b.query.bool.filter) {
                b.query.bool.filter = b.query.bool.filter.filter(filter => {
                    return (!(filter.nested && ['indicators', 'ot.scores'].indexOf(filter.nested.path) >= 0)) && (filter.type !== 'weighted');
                });
                if (b.query.bool.filter.length === 0) {
                    b.query = { match_all: {} };
                }
            }

            aggs = Queries.buildAggregations(['histogram', 'terms_main_cpv_divisions']);
            b.aggregations = aggs.request;

            this.store.Tender.search(b, 0, 0, (err, full_result) => {
                if (err) {
                    console.error(err);
                    return cb(err);
                }
                let data = aggs.parse(full_result.aggregations, { library: this.library, lang: options.lang });

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
                            avg_finalPriceEUR: stats_histogram[year].avg_finalPriceEUR,
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
                        name: c.name,
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
        aggs.request['terms_authority_nuts'].aggregations.authority_nuts1_nested.aggregations = {
            'tender_denested': {
                'reverse_nested': {},
                'aggregations': nuts_stats_aggs.request,
            },
        };
        aggs.request['terms_authority_nuts'].aggregations.authority_nuts2_nested.aggregations = {
            'tender_denested': {
                'reverse_nested': {},
                'aggregations': nuts_stats_aggs.request,
            },
        };
        let b = Queries.buildCountrySearchBody(options, country_id, false);
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
                        value: bucket.doc_count,
                    },
                    stats: cpv_stats_aggs.parse(bucket, { library: this.library, lang: options.lang }),
                };
            });
            let region_stats = data.aggregations['terms_authority_nuts'].authority_nuts1_nested.buckets.concat(data.aggregations['terms_authority_nuts'].authority_nuts2_nested.buckets).map(bucket => {
                return {
                    id: bucket.key,
                    value: bucket.doc_count,
                    stats: nuts_stats_aggs.parse(bucket.tender_denested, { library: this.library, lang: options.lang }),
                };
            });
            let stats = aggs.parse(data.aggregations, { library: this.library, lang: options.lang });

            stats.terms_main_cpv_divisions_score = {};
            if (stats.terms_main_cpv_divisions_scores) {
                Object.keys(stats.terms_main_cpv_divisions_scores).forEach(key => {
                    let part = stats.terms_main_cpv_divisions_scores[key];
                    if (part.scores['TENDER'] !== null) {
                        stats.terms_main_cpv_divisions_score[key] = {
                            name: part.name,
                            value: part.scores['TENDER'],
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
        let b = Queries.buildCountrySearchBody(options, country_id, false);
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
            'terms_authority_nuts',
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
            let stats = aggs.parse(data.aggregations, { library: this.library, lang: options.lang });

            if (subaggregationId) {
                let answer = Queries.getAnswerAggreggation(data.aggregations, subaggregationId);
                let result = answer.buckets.map(bucket => {
                    let sector = {
                        id: bucket.key,
                        name: this.library.getCPVName(bucket.key, options.lang),
                        valid: this.library.isKnownCPV(bucket.key),
                        value: bucket.doc_count,
                    };
                    return {
                        sector: sector,
                        stats: stats_aggs.parse(bucket, { library: this.library, lang: options.lang }),
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
                    b.query = { match_all: {} };
                }
            } else if (b.query === levelQuery) {
                b.query = { match_all: {} };
            }
            aggs = Queries.buildAggregations(['histogram']);
            b.aggregations = aggs.request;

            this.store.Tender.search(b, 0, 0, (err, full_result) => {
                if (err) {
                    console.error(err);
                    return cb(err);
                }
                let full_data = aggs.parse(full_result.aggregations, { library: this.library, lang: options.lang });

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
                            avg_finalPriceEUR: stats_histogram[year].avg_finalPriceEUR,
                        };
                    }
                });
                stats.histogram_finalPriceEUR = undefined;
                stats.histogram_count_finalPrices = histo_pc_per_year;

                this.fillAggregationBodies(stats, (err) => {
                    if (err) {
                        return cb(err);
                    }
                    cb(null, { sector: cpvInfo.cpv, parents: cpvInfo.parents, stats: stats });
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
            'histogram_finalPriceEUR',
            'histogram_indicators',
            'histogram']);
        if (nutsInfo.nuts.level < 3) {
            let term = {};
            let field = 'buyers.address.ot.nutscode.nuts' + nutsInfo.nuts.level;
            term[field] = nutsInfo.nuts.id;
            aggs.request.terms_subregions = {
                'nested': {
                    'path': 'buyers',
                },
                'aggregations': {
                    'buyers_filter': {
                        'filter': {
                            'query': {
                                'bool': {
                                    'filter': [
                                        { 'term': term },
                                    ],
                                },
                            },
                        },
                        'aggregations': {
                            'nuts': {
                                'terms': {
                                    'field': 'buyers.address.ot.nutscode.nuts' + (nutsInfo.nuts.level + 1),
                                    'size': Queries.MAX_NUTS_AGGREGATION,
                                },
                                'aggregations': {
                                    'tenders': {
                                        'reverse_nested': {},
                                    },
                                },
                            },
                        },
                    },
                },
            };
        }

        b.aggregations = aggs.request;
        this.store.Tender.search(b, 0, 0, (err, result) => {
            if (err) {
                return cb(err);
            }
            let stats = aggs.parse(result.aggregations, { library: this.library, lang: options.lang });
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
                                name: this.library.getNUTSName(code),
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
                let benchmark_aggs = Queries.buildAggregations([
                    'histogram_finalPriceEUR',
                    'histogram_indicators',
                ]);
                let benchmark_q = Queries.buildCountrySearchBody({ filters: options.filters }, country_id, false);
                let benchmark_body = { query: benchmark_q.query, aggregations: benchmark_aggs.request };
                this.store.Tender.search(benchmark_body, 0, 0, (err, benchmark_result) => {
                    stats.benchmark = benchmark_aggs.parse(benchmark_result.aggregations, {
                        library: this.library,
                        lang: options.lang,
                    });
                    cb(null, { region: nutsInfo.nuts, parents: nutsInfo.parents, children: children, stats: stats });
                });
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
        let subaggs = Queries.buildAggregations(['terms_authority_nuts', 'top_terms_authorities', 'top_sum_finalPrice_authorities', 'histogram_finalPriceEUR', 'histogram_indicators', 'histogram_distribution_indicators']);
        b.aggregations = aggs.request;
        b.aggregations.lotsbids = {
            'nested': {
                'path': 'lots.bids',
            },
            'aggregations': {
                'lotsbids_winning_nested_filter': {
                    'filter': {
                        'term': {
                            'lots.bids.isWinning': true,
                        },
                    },
                    'aggregations': {
                        'lotsbids_nested': {
                            'nested': {
                                'path': 'lots.bids.bidders',
                            },
                            'aggregations': {
                                'lotsbids_nested_filter': {
                                    'filter': {
                                        'terms': {
                                            'lots.bids.bidders.id': ids,
                                        },
                                    },
                                    'aggregations': {
                                        'tender_denested': {
                                            'reverse_nested': {},
                                            'aggregations': subaggs.request,
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        };

        this.store.Tender.search(b, 0, 0, (err, result) => {
            if (err) {
                return cb(err);
            }
            let stats = aggs.parse(result.aggregations, { library: this.library, lang: options.lang });
            let answer_winning = subaggs.parse(result.aggregations.lotsbids.lotsbids_winning_nested_filter.lotsbids_nested.lotsbids_nested_filter.tender_denested, {
                library: this.library,
                lang: options.lang,
            });
            Object.keys(answer_winning).forEach(key => {
                stats[key] = answer_winning[key];
            });
            this.fillAggregationBodies(stats, (err) => {
                if (err) {
                    return cb(err);
                }
                let benchmark_aggs = Queries.buildAggregations([
                    'histogram_finalPriceEUR',
                    'histogram_indicators',
                ]);
                let benchmark_q = Queries.buildCountrySearchBody({ filters: options.filters }, country_id, false);
                let benchmark_body = { query: benchmark_q.query, aggregations: benchmark_aggs.request };
                this.store.Tender.search(benchmark_body, 0, 0, (err, benchmark_result) => {
                    stats.benchmark = benchmark_aggs.parse(benchmark_result.aggregations, {
                        library: this.library,
                        lang: options.lang,
                    });
                    cb(null, { stats: stats });
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
            'terms_indicators_score',
            'histogram_distribution_indicators'
        ]);
        b.aggregations = aggs.request;
        this.store.Tender.search(b, 0, 0, (err, result) => {
            if (err) {
                console.error(err);
                return cb(err);
            }
            let stats = aggs.parse(result.aggregations, { library: this.library, lang: options.lang });

            this.fillAggregationBodies(stats, (err) => {
                if (err) {
                    return cb(err);
                }
                let benchmark_q = Queries.buildCountrySearchBody({ filters: options.filters }, country_id, false);
                let benchmark_aggs = Queries.buildAggregations([
                    'histogram_finalPriceEUR',
                    'histogram_indicators',
                ]);
                let benchmark_body = { query: benchmark_q.query, aggregations: benchmark_aggs.request };
                this.store.Tender.search(benchmark_body, 0, 0, (err, benchmark_result) => {
                    stats.benchmark = benchmark_aggs.parse(benchmark_result.aggregations, {
                        library: this.library,
                        lang: options.lang,
                    });
                    cb(null, { stats: stats });
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
        this.getAggregation(country_id, 'terms_authority_nuts', 'en', (err, data) => {
            for (let nut in data) {
                data[nut] = {
                    count: data[nut].count,
                    value: data[nut].value,
                };
            }
            cb(err, data);
        });
    }

    getHomeStats(country_id, cb) {
        this.getAggregations(country_id, ['histogram'], 'en', cb);
    }

    getTenderStats(options, country_id, cb) {
        let benchmark_aggs = Queries.buildAggregations([
            'histogram_distribution_indicators',
        ]);
        let benchmark_q = Queries.buildCountrySearchBody({ filters: options.filters }, country_id, false);
        let benchmark_body = { query: benchmark_q.query, aggregations: benchmark_aggs.request };
        this.store.Tender.search(benchmark_body, 0, 0, (err, benchmark_result) => {
            let benchmark = benchmark_aggs.parse(benchmark_result.aggregations, {
                library: this.library,
                lang: options.lang,
            });
            cb(null, { stats: benchmark });
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
            country_term = { term: { 'ot.country': country_id } };
        } else if (entity === 'company') {
            index = this.store.Supplier;
            country_term = { term: { 'countries': country_id } };
        } else if (entity === 'authority') {
            index = this.store.Buyer;
            country_term = { term: { 'countries': country_id } };
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
                match_phrase_prefix: {},
            };
            query_field_pure.match_phrase_prefix[field] = search;
            if (nested) {
                query_field = {
                    'nested': {
                        'path': nested,
                        query: query_field_pure,
                    },
                };
            } else {
                query_field = query_field_pure;
            }
        }

        let aggfield = {
            'result': {
                terms: {
                    field: field + '.raw',
                    size: 100,
                },

            },
        };
        if (nested) {
            aggfield = {
                nestedresult: {
                    'nested': {
                        'path': nested,
                    },
                    'aggregations': {
                        'nested-filter': {
                            'filter': {
                                'query': {
                                    'bool': {
                                        'filter': [
                                            query_field_pure,
                                        ],
                                    },
                                },
                            },
                            'aggregations': aggfield,
                        },
                    },
                },
            };
        }

        let body = {
            query: {
                bool: {
                    filter: [],
                },
            },
            aggregations: aggfield,
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
                return { key: item.key, value: item.doc_count };
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
                    const suppliers = [];
                    (tender.lots || []).forEach((lot) => {
                        (lot.bids || []).forEach((bid) => {
                            (bid.bidders || []).forEach((supplier) => {
                                suppliers.push(supplier);
                            });
                        });
                    });
                    tender.suppliers = suppliers.map((supplier) => ({
                        id: supplier.id,
                        name: supplier.name,
                        address: supplier.address.rawAddress,
                        bidderType: supplier.bidderType,
                    }));
                    this.fillTenderData(tender, options.lang);
                    cb(null, tender);
                }
            }
        });
    }

    streamSearchJSON(size, stream, options, country_id, canContinueFN) {
        let query = Queries.buildCountrySearchBody(options, country_id, false);
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
                      Utils.cleanOtFields(item._source);
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

    streamTenderJSON(id, req, res, options, country_id, onEnd) {
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

        this.streamSearchJSON(100, stream, options, country_id, () => {
            return !closed;
        });
    }

    streamSearchCSV(size, stream, options, country_id, canContinueFN) {
        let query = Queries.buildCountrySearchBody(options, country_id, false);
        let csv = new CSV(this.library);
        stream.write(csv.header());
        let nr = 0;
        this.store.Tender.streamQuery(size, query.query,
          (items) => {
              items.forEach(item => {
                  stream.write(csv.transform(item._source, nr));
                  nr++;
              });
              return !(canContinueFN && !canContinueFN());

          },
          (err) => {
              stream.end();
          });
    }

    streamTenderCSV(id, req, res, options, country_id, onEnd) {
        res.setHeader('Content-Type', 'application/gzip');
        res.setHeader('Cache-Control', 'no-cache');
        res.setHeader('Content-disposition', 'attachment; filename=' + id + '.tenders.csv.gz');

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

        this.streamSearchCSV(100, stream, options, country_id, () => {
            return !closed;
        });
    }

    searchTender(options, country_id, cb) {
        let body = Queries.buildCountrySearchBody(options, country_id, true);
        let aggs = null;
        if (options.stats) {
            let agg_ids = options.stats.filter(id => !!Queries.Aggregations[id]);
            if (agg_ids.length > 0) {
                aggs = Queries.buildAggregations(agg_ids);
                if (Object.keys(aggs.request).length > 0) {
                    body.aggregations = body.aggregations || {};
                    Object.keys(aggs.request).forEach(key => {
                        body.aggregations[key] = aggs.request[key];
                    });
                }
            }
        }
        this.store.Tender.search({
            aggregations: body.aggregations,
        }, 0, 0, (err, aggResult) => {
            this.store.Tender.search(body, (options.size || 10), (options.from || 0), (err, result) => {
                if (err) {
                    return cb(err);
                }
                let stats = undefined;
                if (aggs) {
                    stats = aggs.parse(aggResult.aggregations, { library: this.library, lang: options.lang });
                }
                let aggregations = {};
                Queries.parseSearchAggregations(aggResult.aggregations, {
                    library: this.library,
                    lang: options.lang,
                }, result.aggregations);
                if (aggResult.aggregations) {
                    Object.keys(aggResult.aggregations).forEach(key => {
                        if (key[0] === '_') {
                            aggregations[key] = aggResult.aggregations[key];
                        }
                    });
                }
                let answer = {
                    hits: {
                        total: result.hits.total,
                        hits: result.hits.hits.map(doc => {
                            this.fillTenderData(doc._source, options.lang);
                            return doc._source;
                        }),
                    },
                    aggregations: aggregations,
                    stats: stats,
                };
                if (body.sort) {
                    let key = Object.keys(body.sort)[0];
                    answer.sortBy = { id: key, ascend: body.sort[key].order === 'asc' };
                }
                cb(null, answer);
            });
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

    getCPV(options, cb) {
        let id = Utils.validateId(options.id);
        if (!id) {
            return cb(404);
        }
        let cpvInfo = this.library.parseCPVs(id, options.lang);
        if (!cpvInfo.cpv) {
            return cb(404);
        }
        cb(null, { sector: cpvInfo.cpv, parents: cpvInfo.parents });
    }

    /**
     company & authority dbs
     */

    searchBuyer(options, country_id, cb) {
        let body = Queries.buildSearchBody(options, true, true);
        if (country_id) {
            body = Queries.addCountrySearchBody(body, { 'countries': country_id });
        }
        this.store.Buyer.search({
            aggregations: body.aggregations,
        }, 0, 0, (err, aggResult) => {
            if (err) {
                return cb(err);
            }
            this.store.Buyer.search(body, options.size || 10, options.from || 0, (err, result) => {
                if (err) {
                    return cb(err);
                }
                Queries.parseSearchAggregations(aggResult.aggregations, {
                    library: this.library,
                    lang: options.lang,
                }, result.aggregations);
                // console.log(JSON.stringify(result.hits, null, 4));
                let answer = {
                    hits: {
                        total: result.hits.total,
                        hits: result.hits.hits.map(hit => hit._source),
                    },
                    aggregations: aggResult.aggregations,
                };
                if (body.sort) {
                    let key = Object.keys(body.sort)[0];
                    answer.sortBy = { id: key, ascend: body.sort[key].order === 'asc' };
                }
                cb(null, answer);
            });
        });
    }

    searchSupplier(options, country_id, cb) {
        let body = Queries.buildSearchBody(options, true, true);
        if (country_id) {
            body = Queries.addCountrySearchBody(body, { 'countries': country_id });
        }

        this.store.Supplier.search({
            aggregations: body.aggregations,
        }, 0, 0, (err, aggResult) => {
            if (err) {
                return cb(err);
            }
            this.store.Supplier.search(body, options.size || 10, options.from || 0, (err, result) => {
                if (err) {
                    return cb(err);
                }
                Queries.parseSearchAggregations(aggResult.aggregations, {
                    library: this.library,
                    lang: options.lang,
                }, result.aggregations);
                let answer = {
                    hits: {
                        total: result.hits.total,
                        hits: result.hits.hits.map(hit => hit._source),
                    },
                    aggregations: aggResult.aggregations,
                };
                if (body.sort) {
                    let key = Object.keys(body.sort)[0];
                    answer.sortBy = { id: key, ascend: body.sort[key].order === 'asc' };
                }
                cb(null, answer);
            });
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
                                    'body.name.slug': name || '',
                                },
                            },
                        ],
                    },
                },
            };
            if (ignoreIds && ignoreIds.length > 0) {
                body.query.bool.must_not = {
                    terms: { 'body.id': ignoreIds },
                };
            }
            if (country_id) {
                body.query.bool.must.push({ 'term': { 'countries': country_id } });
            }

            this.store.Supplier.search(body, 200, 0, (err, results) => {
                if (err) {
                    return cb(err);
                }
                let similar = results.hits.hits.map(hit => {
                    return {
                        value: hit._source.count,
                        body: hit._source.body,
                        country: hit._source.body && hit._source.body.address ? hit._source.body.address.country : null,
                    };
                });
                cb(null, { similar: similar });
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
                                    'body.name.slug': name || '',
                                },
                            },
                        ],
                    },
                },
            };

            if (ignoreIds && ignoreIds.length > 0) {
                body.query.bool.must_not = {
                    terms: { 'body.id': ignoreIds },
                };
            }

            if (country_id) {
                body.query.bool.must.push({ 'term': { 'countries': country_id } });
            }
            this.store.Buyer.search(body, 200, 0, (err, results) => {
                if (err) {
                    return cb(err);
                }
                let similar = results.hits.hits.map(hit => {
                    return {
                        value: hit._source.count,
                        body: hit._source.body,
                        country: hit._source.body && hit._source.body.address ? hit._source.body.address.country : null,
                    };
                });
                cb(null, { similar: similar });
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
            cb(null, { company: result.hits.hits[0]._source });
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
            cb(null, { authority: result.hits.hits[0]._source });
        });
    }

    /**
     init
     */

    init(cb) {
        this.store.init(err => {
            cb(err);
        });
    };


}

module.exports = Api;
