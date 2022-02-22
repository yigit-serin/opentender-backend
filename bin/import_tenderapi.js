#!/usr/bin/env node

/*

 Import tender api packages into the DB

 - clears the DB
 - checks tenderapi packages whith JSON Schema
 - stores Tender into DB tender
 - stores Tender.buyers into DB authority
 - stores Tender.lots.bids.bidders into DB company

 */

const path = require('path');
const fs = require('fs');
const async = require('async');
const Ajv = require('ajv');
const status = require('node-status');
const cloneDeep = require('lodash.clonedeep');

const console = status.console();

let status_tenders = status.addItem('tenders', {type: ['count']});
let status_suppliers = status.addItem('suppliers', {type: ['count']});
let status_buyers = status.addItem('buyers', {type: ['count']});

const Store = require('../lib/store.js');
const Library = require('../lib/library.js');
const Converter = require('../lib/convert.js');
const Utils = require('../lib/utils');

const config = require('../config.js');
const data_path = config.data.tenderapi;
const store = new Store(config);
const library = new Library(config);
const converter = new Converter(null, library, config.data.path);
let tender_count = 0;
let stats = {};

const validator = (filename) => {
    const ajv = new Ajv({verbose: true, jsonPointers: true, allErrors: true});
    const schema = JSON.parse(fs.readFileSync(path.resolve(config.data.shared, filename)).toString());
    return ajv.compile(schema);
};

const validateOpentender = validator('schema.json');
const validateTenderAPI = validator('tenderapi.json');

if (!Array.prototype.flatMap) {
    // eslint-disable-next-line no-extend-native
    Array.prototype.flatMap = function (f, ctx) {
        return this.reduce((r, x, i, a) => r.concat(f.call(ctx, x, i, a)), []);
    };
}

let clearIndex = (index, cb) => {
    async.waterfall([
        (next) => index.removeIndex(next),
        (next) => index.checkIndex(next),
    ], (err) => {
        cb(err);
    });
};

let openDB = (cb) => {
    async.waterfall([
        (next) => store.init(next),
        (next) => clearIndex(store.Tender, next),
        (next) => clearIndex(store.Buyer, next),
        (next) => clearIndex(store.Supplier, next),
    ], (err) => {
        cb(err);
    });
};

let safeBulkPackages = (array) => {
    let bulk_packages = [];
    while (array.length > 0) {
        let bulk_package = array.slice(0, 1000);
        bulk_packages.push(bulk_package);
        array = array.slice(1000);
    }
    return bulk_packages;
};

let importBulk = (array, index, status, cb) => {
    if (array.length === 0) {
        return cb();
    }
    let bulk_packages = safeBulkPackages(array);

    async.forEachSeries(bulk_packages, (bulk_package, next) => {
        index.bulk_add(bulk_package, (err) => {
            if (!err) {
                status.inc(bulk_package.length);
            }
            next(err);
        });
    }, (err) => {
        cb(err);
    });
};

let updateBulk = (array, index, cb) => {
    if (array.length === 0) {
        return cb();
    }
    let bulk_packages = safeBulkPackages(array);
    async.forEachSeries(bulk_packages, (bulk_package, next) => {
        index.bulk_update(bulk_package, (err) => {
            next(err);
        });
    }, (err) => {
        cb(err);
    });
};

let importTenderPackage = (array, filename, cb) => {
    let valid = validateTenderAPI(array);
    if (!valid) {
        return cb({msg: 'tenderapi schema error in filename ' + filename, errors: validateTenderAPI.errors});
    }

    console.log('Dataset is valid! Converting the dataset...');

    array = converter.transform(array);

    console.log('Dataset is converted. Checking converted data...');

    valid = validateOpentender(array);
    if (!valid) {
        return cb({msg: 'opentender schema error in filename ' + filename, errors: validateOpentender.errors});
    }

    console.log('Converted data is valid! Importing dataset...');

    array.forEach(item => {
        stats[item.country] = (stats[item.country] || 0) + 1;
    });

    array.forEach(item => {
        item.ot.indicators = Object.entries(item.ot.indicator).map(([indicator, value]) => ({
            type: indicator,
            value,
            status: 'CALCULATED',
        }));
    });

    async.waterfall([
        (next) => importBuyers(array, next),
        (next) => importSuppliers(array, next),
        (next) => importBulk(array, store.Tender, status_tenders, next),
    ], (err) => {
        cb(err);
    });
};

let importTenderPackageFile = (filename, cb) => {
    // read & decompress a package file
    let fullfilename = path.join(data_path, 'import', filename);
    fs.readFile(fullfilename, (err, content) => {
        if (err) {
            return cb(err);
        }
        let array = JSON.parse(content.toString());
        status_tenders.max = array.length;
        importTenderPackage(array, filename, cb);
    });
};

let calculateContractsCountToBuyer = (buyer, item) => {
    if (!item || !buyer) {
        return;
    }

    if (item.lots) {
        if (!buyer.body.contractsCount) {
            buyer.body.contractsCount = 0;
        }
        buyer.body.contractsCount += item.lots.reduce((sum, lot) => {
            return sum + (lot.bids ? 1 : 0);
        }, 0);
    }
};

let calculateCpvCodesToBuyer = (buyer, item) => {
    if (!item || !buyer) {
        return;
    }
    if (!buyer.body) {
        buyer.body = {};
    }
    if (!buyer.body.sector) {
        buyer.body.sector = {};
    }
    if (!buyer.body.sector.cpvs) {
        buyer.body.sector.cpvs = [];
    }

    if (!item.buyers || !item.buyers.some((buyer) => buyer.id === buyer.id)) {
        return;
    }

    if (!item.cpvs) {
        return;
    }

    item.cpvs.forEach((cpv) => {
        if (!buyer.body.sector.cpvs.includes(cpv.code)) {
            buyer.body.sector.cpvs.push(cpv.code);
        }
    });
};

let calculateAwardDecisionDatesToBuyer = (buyer, item) => {
    if (!item || !buyer) {
        return;
    }

    if (!buyer.body) {
        buyer.body = {};
    }
    if (!buyer.body.dates) {
        buyer.body.dates = {};
    }
    if (!buyer.body.dates.awardDecisionDates) {
        buyer.body.dates.awardDecisionDates = [];
    }
    if (!buyer.body.dates.awardDecisionYears) {
        buyer.body.dates.awardDecisionYears = [];
    }

    if (!item.lots) {
        return;
    }
    if (!item.buyers || !item.buyers.some((buyer) => buyer.id === buyer.id)) {
        return;
    }

    item.lots.forEach((lot) => {
        if (!lot.awardDecisionDate) {
            return;
        }
        if (!buyer.body.dates.awardDecisionDates.includes(lot.awardDecisionDate)) {
            buyer.body.dates.awardDecisionDates.push(lot.awardDecisionDate);
        }
        const year = new Date(lot.awardDecisionDate).getFullYear();
        if (!buyer.body.dates.awardDecisionYears.includes(year)) {
            buyer.body.dates.awardDecisionYears.push(year);
        }
    });
};

let calculateMostFrequentMarketToBuyer = (buyer, item) => {
    if (!item || !buyer) {
        return;
    }
    if (!buyer.body) {
        buyer.body = {};
    }
    if (!buyer.body.sector) {
        buyer.body.sector = {};
    }

    if (!item.buyers || !item.buyers.some((buyer) => buyer.id === buyer.id)) {
        return;
    }

    if (!item.cpvs) {
        return;
    }
    buyer.mostFrequentMarketMap = new Map();

    item.cpvs.forEach((cpv) => {
        const shortenCpv = cpv.code.toString().slice(0, 2);
        if (!buyer.mostFrequentMarketMap.has(shortenCpv)) {
            buyer.mostFrequentMarketMap.set(shortenCpv, 0);
        }
        buyer.mostFrequentMarketMap.set(shortenCpv, buyer.mostFrequentMarketMap.get(shortenCpv) + 1);
    });
};

let addCountryToBuyer = (buyer, tender) => {
    if (!tender || !buyer) {
        return;
    }
    if (buyer.body.address && buyer.body.address.country) {
        return;
    }
    if (buyer.body.address && !buyer.body.address.country) {
        buyer.body.address.country = tender.country;
    }
};

let addCountryToSupplier = (supplier, tender) => {
    if (!tender || !supplier) {
        return;
    }
    if (supplier.body.address && supplier.body.address.country) {
        return;
    }
    if (supplier.body.address && !supplier.body.address.country) {
        supplier.body.address.country = tender.country;
    }
};

let calculateTotalContractsToBuyer = (buyer, item) => {
    if (!item || !buyer) {
        return;
    }
    if (!buyer.body) {
        buyer.body = {};
    }
    if (!buyer.body.company) {
        buyer.body.company = {};
    }
    if (!buyer.body.company.totalValueOfContracts) {
        buyer.body.company.totalValueOfContracts = 0;
    }

    if (!item.buyers || !item.buyers.some((buyer) => buyer.id === buyer.id)) {
        return;
    }

    (item.lots || []).forEach(lot => {
        (lot.bids || []).forEach(bid => {
            if (!bid.digiwhistPrice || !bid.digiwhistPrice.netAmount) {
                return;
            }

            buyer.body.company.totalValueOfContracts += bid.digiwhistPrice.netAmount;
        });
    });
};

function calculateElementaryIndicators(type, tender) {
    const filterIndicators = (indicator) => indicator.type && indicator.type.startsWith(type) && indicator.status === 'CALCULATED';
    const extractIndicatorValue = (indicator) => indicator.value;

    const lotIndicators = (tender.lots || []).flatMap((lot) =>
      (lot.indicators || []).filter(filterIndicators).flatMap(extractIndicatorValue),
    );
    const tenderIndicators = Object.entries(tender.ot.indicator || {})
      .map(([key, value]) => ({type: key, value, status: 'CALCULATED'}))
      .filter(filterIndicators).map(extractIndicatorValue);
    return {
        tender: Math.floor(tenderIndicators.length && tenderIndicators.reduce((acc, value) => acc + value, 0) / tenderIndicators.length * 10000) / 10000,
        lot: Math.floor(lotIndicators.length && lotIndicators.reduce((acc, value) => acc + value, 0) / lotIndicators.length * 10000) / 10000,
    };
}

let importBuyers = (items, cb) => {
    if (items.length === 0) {
        return cb();
    }
    let buyers = [];
    items = converter.cleanTenders(items);
    items.forEach(item => {
        (item.buyers || []).forEach((body, index) => {
            body.id = body.id || 'no-id';
            let buyer = buyers.find(b => {
                return b.body.id === body.id;
            });
            if (!buyer) {
                buyer = {
                    id: body.id,
                    body: body,
                    countries: [],
                    count: 0,
                };
                buyers.push(buyer);
            }

            calculateContractsCountToBuyer(buyer, item);
            addCountryToBuyer(buyer, item);
            calculateCpvCodesToBuyer(buyer, item);
            calculateAwardDecisionDatesToBuyer(buyer, item);
            calculateMostFrequentMarketToBuyer(buyer, item);
            calculateTotalContractsToBuyer(buyer, item);
            item.buyers[index].totalValueOfContracts = buyer.body.company.totalValueOfContracts;
            buyer.body.indicator = {};
            buyer.ot = {};
            buyer.ot.indicators = item.ot.indicators;
            buyer.body.indicator.elementaryIntegrityIndicators = calculateElementaryIndicators('INTEGRITY_', item);
            buyer.body.indicator.elementaryTransparencyIndicators = calculateElementaryIndicators('TRANSPARENCY_', item);
            buyer.body.indicator.transparencyIndicatorCompositionScore = Math.floor(
              (buyer.body.indicator.elementaryTransparencyIndicators.tender +
                buyer.body.indicator.elementaryTransparencyIndicators.lot) / 2 * 10000,
            ) / 10000;
            buyer.body.indicator.integrityIndicatorCompositionScore = Math.floor(
              (buyer.body.indicator.elementaryIntegrityIndicators.tender +
                buyer.body.indicator.elementaryIntegrityIndicators.lot) / 2 * 10000,
            ) / 10000;

            item.buyers[index].indicator = buyer.body.indicator;
            item.buyers[index].totalValueOfContracts = buyer.body.company.totalValueOfContracts;

            if (buyer.countries.indexOf(item.ot.country) < 0) {
                buyer.countries.push(item.ot.country);
            }
            buyer.count++;
        });
    });
    buyers.forEach((buyer) => {
        const cpvs = Array.from(buyer.mostFrequentMarketMap.entries());
        let cpv = cpvs[0];
        cpvs.forEach(([code, count]) => {
            if (cpv[1] < count) {
                cpv = [code, count];
            }
        });
        const label = library.getCPVName(cpv[0]);
        delete buyer.mostFrequentMarketMap;
        buyer.body.sector.mostFrequentMarket = {
            key: cpv[0],
            label,
        };
        buyer.body.sector.cpvCodes = buyer.body.sector.cpvs.map((cpv) => cpv);
        buyer.body.sector.cpvs = buyer.body.sector.cpvs.map((cpv) => {
            return {
                key: cpv,
                label: library.getCPVName(cpv),
            };
        });

        const yearsMin = Math.min(...buyer.body.dates.awardDecisionYears);
        const yearsMax = Math.max(...buyer.body.dates.awardDecisionYears);

        buyer.body.dates.awardDecisionYearsMinMax = '';
        if (Math.abs(yearsMin) !== Infinity) {
            buyer.body.dates.awardDecisionYearsMinMax += `${yearsMin} - `;
        }
        if (Math.abs(yearsMax) !== Infinity) {
            buyer.body.dates.awardDecisionYearsMinMax += `${yearsMax}`;
        }
    });
    let ids = buyers.map(buyer => {
        return buyer.body.id;
    });
    buyers.forEach((buyer) => {
     buyer.body.company = {}
     buyer.ot.indicator = {}
     buyer.body.company.totalValueOfContracts = 0
     let count = 0
     items.forEach((item) => {
      let valid = false;

      (item.buyers || []).forEach((body, index) => {
       if (body.id === buyer.id) {
        valid = true
        count++
       }
      })

      if (valid && item.ot && item.ot.indicator) {
       Object.keys(item.ot.indicator).forEach((key) => {
        if (!buyer.ot.indicator[key]) {
         buyer.ot.indicator[key] = 0
        }

        buyer.ot.indicator[key] += item.ot.indicator[key]
       })
      }

      if (valid && item.lots && item.lots.length) {
       item.lots.forEach((lot) => {
        if (lot.estimatedPrice) {
         buyer.body.company.totalValueOfContracts += lot.finalPrice.netAmountNational
        }
       })
      }
     })
     buyer.body.company.totalValueOfContracts /= 100
     buyer.body.company.totalValueOfContracts = Utils.roundValueTwoDecimals(buyer.body.company.totalValueOfContracts)

     Object.keys(buyer.ot.indicator).forEach((key) => {
      buyer.ot.indicator[key] = Utils.roundValueTwoDecimals(buyer.ot.indicator[key] / count)
     })
     buyer.ot.indicators = Object.entries(buyer.ot.indicator).map(([indicator, value]) => ({
      type: indicator,
      value,
      status: 'CALCULATED'
     }))

     items.forEach((item) => {
      (item.buyers || []).forEach((body, index) => {
       if (body.id === buyer.id) {
        item.buyers[index].totalValueOfContracts = buyer.body.company.totalValueOfContracts
       }
      })
     })
    });
    store.Buyer.getByIds(ids, (err, result) => {
        if (err) return cb(err);
        let new_list = [];
        let update_hits = [];
        buyers.forEach(buyer => {
            let hit = result.hits.hits.find(h => {
                return buyer.body.id === h._source.body.id;
            });
            if (hit) {
                update_hits.push(hit);
            } else {
                new_list.push(buyer);
            }
        });
        updateBulk(update_hits, store.Buyer, (err) => {
            if (err) return cb(err);
            importBulk(new_list, store.Buyer, status_buyers, (err) => {
                cb(err);
            });
        });
    });

};

let calculateContractsCountToSupplier = (supplier, item) => {
    if (!item || !supplier) {
        return;
    }

    if (item.lots) {
        if (!supplier.body.contractsCount) {
            supplier.body.contractsCount = 0;
        }
        supplier.body.contractsCount += item.lots.reduce((sum, lot) => {
            return sum + (lot.bids ? 1 : 0);
        }, 0);
    }
};

let calculateAwardDecisionDatesToSupplier = (supplier, item) => {
    if (!item || !supplier) {
        return;
    }

    if (!supplier.body) {
        supplier.body = {};
    }
    if (!supplier.body.dates) {
        supplier.body.dates = {};
    }
    if (!supplier.body.dates.awardDecisionDates) {
        supplier.body.dates.awardDecisionDates = [];
    }
    if (!supplier.body.dates.awardDecisionYears) {
        supplier.body.dates.awardDecisionYears = [];
    }

    if (!item.lots) {
        return;
    }
    if (!item.buyers || !item.buyers.some((buyer) => buyer.id === buyer.id)) {
        return;
    }

    item.lots.forEach((lot) => {
        if (!lot.awardDecisionDate) {
            return;
        }
        if (!supplier.body.dates.awardDecisionDates.includes(lot.awardDecisionDate)) {
            supplier.body.dates.awardDecisionDates.push(lot.awardDecisionDate);
        }
        const year = new Date(lot.awardDecisionDate).getFullYear();
        if (!supplier.body.dates.awardDecisionYears.includes(year)) {
            supplier.body.dates.awardDecisionYears.push(year);
        }
    });
};

let calculateCpvCodeToSupplier = (supplier, item) => {
    if (!item || !supplier) {
        return;
    }
    if (!supplier.body) {
        supplier.body = {};
    }
    if (!supplier.body.sector) {
        supplier.body.sector = {};
    }
    if (!supplier.body.sector.cpvs) {
        supplier.body.sector.cpvs = [];
    }

    if (!item.cpvs) {
        return;
    }

    item.cpvs.forEach((cpv) => {
        if (!supplier.body.sector.cpvs.includes(cpv.code)) {
            supplier.body.sector.cpvs.push(cpv.code);
        }
    });
};

let calculateMostFrequentMarketToSupplier = (supplier, item) => {
    if (!item || !supplier) {
        return;
    }
    if (!supplier.body) {
        supplier.body = {};
    }
    if (!supplier.body.sector) {
        supplier.body.sector = {};
    }

    if (!item.cpvs) {
        return;
    }
    supplier.mostFrequentMarketMap = new Map();

    item.cpvs.forEach((cpv) => {
        const shortenCpv = cpv.code.toString().slice(0, 2);
        if (!supplier.mostFrequentMarketMap.has(shortenCpv)) {
            supplier.mostFrequentMarketMap.set(shortenCpv, 0);
        }
        supplier.mostFrequentMarketMap.set(shortenCpv, supplier.mostFrequentMarketMap.get(shortenCpv) + 1);
    });
};

let calculateTotalContractsToSupplier = (supplier, bid) => {
    if (!bid || !supplier) {
        return;
    }
    if (!supplier.body) {
        supplier.body = {};
    }
    if (!supplier.body.company) {
        supplier.body.company = {};
    }
    if (!supplier.body.company.totalValueOfContracts) {
        supplier.body.company.totalValueOfContracts = 0;
    }

    if (!bid.digiwhistPrice || !bid.digiwhistPrice.netAmount) {
        return;
    }

    supplier.body.company.totalValueOfContracts += bid.digiwhistPrice.netAmount;
};

let importSuppliers = (items, cb) => {
    if (items.length === 0) {
        return cb();
    }
    let suppliers = [];
    items.forEach(item => {
        (item.lots || []).forEach((lot, i1) => {
            (lot.bids || []).forEach((bid, i2) => {
                (bid.bidders || []).forEach((body, i3) => {
                    body.id = body.id || 'no-id';
                    let supplier = suppliers.find(b => {
                        return b.body.id === body.id;
                    });
                    if (!supplier) {
                        supplier = {
                            id: body.id,
                            body: body,
                            count: 0,
                            countries: [],
                        };
                        suppliers.push(supplier);
                    }

                    addCountryToSupplier(supplier, item);
                    calculateCpvCodeToSupplier(supplier, item);
                    calculateAwardDecisionDatesToSupplier(supplier, item);
                    calculateMostFrequentMarketToSupplier(supplier, item);
                    calculateTotalContractsToSupplier(supplier, bid);
                    calculateContractsCountToSupplier(supplier, item);
                    item.lots[i1].bids[i2].bidders[i3].contractsCount = supplier.body.contractsCount;
                    item.lots[i1].bids[i2].bidders[i3].totalValueOfContracts = supplier.body.company.totalValueOfContracts;
                    supplier.ot = {};
                    supplier.ot.indicators = item.ot.indicators;
                    supplier.body.indicator = {};
                    supplier.body.indicator.elementaryIntegrityIndicators = calculateElementaryIndicators('INTEGRITY_', item);
                    supplier.body.indicator.elementaryTransparencyIndicators = calculateElementaryIndicators('TRANSPARENCY_', item);
                    supplier.body.indicator.transparencyIndicatorCompositionScore = Math.floor(
                      (supplier.body.indicator.elementaryTransparencyIndicators.tender +
                        supplier.body.indicator.elementaryTransparencyIndicators.lot) / 2 * 10000,
                    ) / 10000;
                    supplier.body.indicator.integrityIndicatorCompositionScore = Math.floor(
                      (supplier.body.indicator.elementaryIntegrityIndicators.tender +
                        supplier.body.indicator.elementaryIntegrityIndicators.lot) / 2 * 10000,
                    ) / 10000;

                    item.lots[i1].bids[i2].bidders[i3].indicator = supplier.body.indicator;
                    item.lots[i1].bids[i2].bidders[i3].totalValueOfContracts = supplier.body.company.totalValueOfContracts;

                    supplier.count++;
                    if (supplier.countries.indexOf(item.ot.country) < 0) {
                        supplier.countries.push(item.ot.country);
                    }
                });
            });
        });
    });
    let ids = suppliers.map(supplier => {
        return supplier.body.id;
    });
    suppliers.forEach((supplier) => {
        const cpvs = Array.from(supplier.mostFrequentMarketMap.entries());
        let cpv = cpvs[0];
        cpvs.forEach(([code, count]) => {
            if (cpv[1] < count) {
                cpv = [code, count];
            }
        });
        const label = library.getCPVName(cpv[0]);
        delete supplier.mostFrequentMarketMap;
        supplier.body.sector.mostFrequentMarket = {
            key: cpv[0],
            label,
        };
        supplier.body.sector.cpvCodes = supplier.body.sector.cpvs.map((cpv) => cpv);
        supplier.body.sector.cpvs = supplier.body.sector.cpvs.map((cpv) => {
            return {
                key: cpv,
                label: library.getCPVName(cpv),
            };
        });

        const yearsMin = Math.min(...supplier.body.dates.awardDecisionYears);
        const yearsMax = Math.max(...supplier.body.dates.awardDecisionYears);

        supplier.body.dates.awardDecisionYearsMinMax = '';
        if (Math.abs(yearsMin) !== Infinity) {
            supplier.body.dates.awardDecisionYearsMinMax += `${yearsMin} - `;
        }
        if (Math.abs(yearsMax) !== Infinity) {
            supplier.body.dates.awardDecisionYearsMinMax += `${yearsMax}`;
        }
    });
    suppliers.forEach((supplier) => {
     supplier.body.company = {}
     supplier.ot.indicator = {}
     supplier.body.company.totalValueOfContracts = 0
     let count = 0
     items.forEach((item) => {
      let valid = false;
      (item.lots || []).forEach((lot) => {
       (lot.bids || []).forEach((bid) => {
        (bid.bidders || []).forEach((body) => {
         if (body.id === supplier.id) {
          valid = true
          count++
         }
        })
       })
      })
      if (valid && item.ot && item.ot.indicator) {
       Object.keys(item.ot.indicator).forEach((key) => {
        if (!supplier.ot.indicator[key]) {
         supplier.ot.indicator[key] = 0
        }

        supplier.ot.indicator[key] += item.ot.indicator[key]
       })
      }
      if (valid && item.lots && item.lots.length) {
       item.lots.forEach((lot) => {
        if (lot.estimatedPrice) {
         supplier.body.company.totalValueOfContracts += lot.finalPrice.netAmountNational
        }
       })
      }
     })
     supplier.body.company.totalValueOfContracts /= 100
     supplier.body.company.totalValueOfContracts = Utils.roundValueTwoDecimals(supplier.body.company.totalValueOfContracts)
     Object.keys(supplier.ot.indicator).forEach((key) => {
      supplier.ot.indicator[key] = Utils.roundValueTwoDecimals(supplier.ot.indicator[key] / count)
     })
     supplier.ot.indicators = Object.entries(supplier.ot.indicator).map(([indicator, value]) => ({
      type: indicator,
      value,
      status: 'CALCULATED'
     }))

     items.forEach((item) => {
      (item.lots || []).forEach((lot, i1) => {
       (lot.bids || []).forEach((bid, i2) => {
        (bid.bidders || []).forEach((body, i3) => {
         if (body.id === supplier.id) {
          item.lots[i1].bids[i2].bidders[i3].totalValueOfContracts = supplier.body.company.totalValueOfContracts
         }
        })
       })
      })
     })
    });
    store.Supplier.getByIds(ids, (err, result) => {
        if (err) return cb(err);
        let new_list = [];
        let update_hits = [];
        suppliers.forEach(supplier => {
            let hit = result.hits.hits.find(h => {
                return supplier.body.id === h._source.body.id;
            });
            if (hit) {
                update_hits.push(hit);
            } else {
                new_list.push(supplier);
            }
        });
        updateBulk(update_hits, store.Supplier, (err) => {
            if (err) return cb(err);
            importBulk(new_list, store.Supplier, status_suppliers, (err) => {
                if (err) return cb(err);
                cb();
            });
        });
    });
};

let importTenderPackageFiles = (filename, cb) => {
    openDB((err) => {
        if (err) {
            return cb(err);
        }
        status.start();
        importTenderPackageFile(filename, err => {
            status.stop();
            if (tender_count > 0) {
                console.log('Tender Country Stats:', JSON.stringify(stats));
            } else {
                console.error('Could not read any tenders');
                store.close(() => {
                    cb(err);
                });
            }
        });
    });
};


importTenderPackageFiles('JM-dataset.json', err => {
// importTenderPackageFiles('KE-dataset-light.json', err => {
// importTenderPackageFiles('tenderapi_0000_2015-01-01T00-00-00-000.json', err => {
    if (err) {
        console.log(err);
    }
    if (!err) {
        console.log('done.');
    }
});
