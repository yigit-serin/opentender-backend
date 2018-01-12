const path = require('path');
const fs = require('fs');
const Utils = require('./utils');

function loadPC2NUTS(datapath) {
	let result = {};
	let folder = path.resolve(datapath, 'pc2016_NUTS-2013');
	let files = fs.readdirSync(folder).filter(file => path.extname(file) === '.json');
	files.forEach(file => {
		let country = file.split('_')[1].toUpperCase();
		let nutso = JSON.parse(fs.readFileSync(path.resolve(folder, file)));
		let collect = {};
		Object.keys(nutso).forEach(nuts => {
			let postcodes = nutso[nuts];
			postcodes.forEach(postcode => collect[postcode] = nuts);
		});
		result[country] = collect;
	});
	return result;
}

function loadNUTSmaps(datapath) {
	let result = [];
	let folder = path.resolve(datapath, 'mappings_NUTS-2013');
	let files = fs.readdirSync(folder).filter(file => path.extname(file) === '.json');
	files.forEach(file => {
		let nuts = JSON.parse(fs.readFileSync(path.resolve(folder, file))).mapping;
		result = result.concat(nuts);
	});
	return result;
}

function loadCPVmap(datapath) {
	let result = {};
	let filename = path.resolve(datapath, 'cpv/mappings_cpvs_2003_2007.csv');
	let lines = fs.readFileSync(filename).toString().split('\n');
	lines.forEach(line => {
		let parts = line.split(';');
		if (parts.length === 2) {
			result[parts[0].slice(0, 8)] = parts[1].slice(0, 10);
		}
	});
	filename = path.resolve(datapath, 'cpv/dkpp2cpv.json');
	let obj = JSON.stringify(fs.readFileSync(filename).toString());
	Object.keys(obj).forEach(key => {
		result[key] = obj[key][0];
	});
	return result;
}

class Converter {

	constructor(stats, library, datapath) {
		this.stats = stats;
		this.library = library;
		this.pc2NUTS = loadPC2NUTS(datapath);
		this.nuts_maps = loadNUTSmaps(datapath);
		this.cpv_map = loadCPVmap(datapath);
		if (this.stats) {
			this.stats.nuts = this.stats.nuts || {count: 0, mappedByPostCode: 0, unknown: {}, known: {}, mapped: {}};
			this.stats.body = this.stats.body || {count: 0, noname: 0};
			this.stats.cpvs = this.stats.cpvs || {count: 0, unknown: {}};
		}
	}

	cleanNUTS(nuts, doc) {
		let result = (nuts || []).filter(nut => nut !== null).map(n => {
			let nut = n.split('-')[0].trim();
			if (!this.library.isKnownNUTSCode(nut)) {
				let mapping_nut = '';
				if (doc.country && doc.postcode) {
					let pc2nuts = this.pc2NUTS[doc.country];
					if (pc2nuts) {
						let postcode = (doc.postcode || '').replace(/ /g, '').replace(/-/g, '');
						if (['MT', 'IE', 'UK'].indexOf(doc.country) < 0) {
							postcode = postcode.split('').filter(c => {
								return !isNaN(c);
							}).join('');
						}
						let code = pc2nuts[postcode];
						if (code && this.library.isKnownNUTSCode(code)) {
							if (this.stats) {
								this.stats.nuts.mappedByPostCode = this.stats.nuts.mappedByPostCode + 1;
							}
							mapping_nut = code;
						}
					}
				}
				if (mapping_nut.length === 0) {
					let map = this.nuts_maps.find(m => nut === m.src);
					if (map && this.library.isKnownNUTSCode(map.dest)) {
						if (this.stats) {
							this.stats.nuts.mapped[nut] = (this.stats.nuts.mapped[nut] || 0) + 1;
						}
						mapping_nut = map.dest;
					} else {
						if (this.stats) {
							this.stats.nuts.unknown[nut] = (this.stats.nuts.unknown[nut] || 0) + 1;
						}
						mapping_nut = '';
					}
				}
				nut = mapping_nut;
			} else {
				if (this.stats) {
					this.stats.nuts.known[nut] = (this.stats.nuts.known[nut] || 0) + 1;
				}
			}
			return nut;
		}).filter(nut => nut.length > 0);
		if (result.length === 0) {
			return undefined;
		}
		return result;
	};

	chooseNUTS(doc) {
		let nutscodes = this.cleanList(doc.nuts);
		if (nutscodes && nutscodes.length > 0) {
			nutscodes = [nutscodes[0]];
		}
		nutscodes = this.cleanNUTS(nutscodes, doc);
		if (nutscodes && nutscodes.length > 0) {
			if (this.stats) {
				this.stats.nuts.count = this.stats.nuts.count + 1;
			}
			doc.ot = {
				nutscode: nutscodes[0]
			};
		}
	}

	shortenProperties(names, o, amount) {
		names.forEach(s => {
			if (o[s] && o[s].length > amount) {
				o[s] = o[s].slice(0, amount);
			}
		});
	};

	cleanProperties(names, o) {
		names.forEach(s => {
			if (o[s]) {
				o[s] = undefined;
			}
		});
	};

	cleanGroupID(id) {
		if (id.indexOf('group_') === 0) {
			return id.slice(6);
		}
		console.log('ALARM', 'what about the freakin groupId', id);
		return id;
	};

	cleanList(list) {
		if (list) {
			let result = list.filter(doc => {
				return doc && Object.keys(doc).length > 0;
			});
			if (result.length > 0) {
				return result;
			}
		}
		return undefined;
	};

	getIndicatorsAsFields(list) {
		let result = {};
		if (list) {
			list.forEach(doc => {
				if (doc.status === 'CALCULATED' && !isNaN(doc.value)) {
					result[doc.type] = doc.value;
				}
			});
		}
		return result;
	}

	cleanIndicators(list) {
		list = this.cleanList(list);
		if (list) {
			list.forEach(doc => {
				if (!isNaN(doc.value)) {
					doc.value = Utils.roundValueFourDecimals(doc.value)
				}
				this.cleanProperties(['@class', 'id', 'relatedEntityId', 'created', 'modified', 'createdBy', 'modifiedBy', 'createdByVersion', 'modifiedByVersion', 'metaData'], doc);
			});
		}
		return list;
	};

	calculateIndicatorScores(list) {
		let result = [];
		let indicators = {};
		if (list) {
			list.forEach(doc => {
				if (doc.status === 'CALCULATED' && !isNaN(doc.value)) {
					let groupId = doc.type.split('_')[0];
					indicators[groupId] = indicators[groupId] || [];
					indicators[groupId].push(doc);
				}
			});
		}
		Object.keys(indicators).forEach(key => {
			let l = indicators[key];
			if (l.length === 0) {
				result.push({type: key, status: 'INSUFFICIENT_DATA'});
			} else {
				let sum = 0;
				l.forEach(doc => {
					sum += doc.value;
				});
				let avg = sum / l.length;
				avg = Utils.roundValueFourDecimals(avg);
				result.push({type: key, value: avg, status: 'CALCULATED'});
			}
		});
		let composite = result.filter(doc => doc.status === 'CALCULATED' && !isNaN(doc.value));
		if (composite.length === 0) {
			result.push({type: 'TENDER', status: 'INSUFFICIENT_DATA'});
		} else {
			let sum = 0;
			composite.forEach(doc => {
				sum += doc.value;
			});
			let avg = sum / composite.length;
			avg = Utils.roundValueFourDecimals(avg);
			result.push({type: 'TENDER', value: avg, status: 'CALCULATED'});
		}
		return result;
	};

	cleanBids(list) {
		list = this.cleanList(list);
		if (list) {
			list.forEach(doc => {
				this.cleanProperties(['sourceBidIds'], doc);
				doc.bidders = this.cleanBodies(doc.bidders);
				doc.subcontractors = this.cleanBodies(doc.subcontractors);
				doc.unitPrices = this.cleanList(doc.unitPrices);
				doc.subcontractedValue = this.cleanPrice(doc.subcontractedValue);
				doc.price = this.cleanPrice(doc.price);
			});
		}
		return list;
	};

	cleanLots(list) {
		list = this.cleanList(list);
		if (list) {
			list.forEach(doc => {
				if (doc.title === doc.description) {
					doc.description = undefined;
				}
				this.cleanProperties(['sourceLotIds'], doc);
				this.shortenProperties(['title'], doc, 4000);
				doc.cpvs = this.cleanCPVS(doc.cpvs);
				doc.fundings = this.cleanList(doc.fundings);
				doc.bids = this.cleanBids(doc.bids);
				doc.estimatedPrice = this.cleanPrice(doc.estimatedPrice);
			});
		}
		return list;
	};

	cleanAddress(doc) {
		if (doc) {
			this.chooseNUTS(doc);
		}
	};

	cleanPublications(list) {
		list = this.cleanList(list);
		if (list) {
			list.forEach(doc => {
				this.cleanProperties(['@class'], doc);
				this.shortenProperties(['buyerAssignedId'], doc, 200);
			});
		}
		return list;
	};

	cleanPrice(doc) {
		if (!doc) {
			return undefined;
		}
		// this.cleanProperties(['publicationDate'], doc);
		if ((doc['netAmountEur'] > 1000000000000)) { //ignore prices larger than 1 trillion
			if (this.stats) {
				this.stats.ignored_prices = this.stats.ignored_prices || [];
				this.stats.ignored_prices.push(doc['netAmountEur']);
			}
			return undefined;
		}
		return doc;
	};

	cleanBody(doc) {
		if (!doc) {
			return undefined;
		}
		doc.id = this.cleanGroupID(doc.groupId);
		this.cleanProperties(['groupId', 'created', 'modified', 'createdBy', 'createdByVersion', 'modifiedBy', 'modifiedByVersion', '_id', 'bodyIds'], doc);
		if (this.stats) {
			this.stats.body.count = this.stats.body.count + 1;
			if (doc.name === undefined || doc.name === '') {
				this.stats.body.noname = this.stats.body.noname + 1;
			}
		}
		if (doc.indicators && doc.indicators.length === 0) {
			doc.indicators = undefined;
		}
		this.cleanAddress(doc.address);
	};

	cleanBodies(list) {
		list = this.cleanList(list);
		if (list) {
			list.forEach(doc => {
				this.cleanBody(doc);
			});
		}
		return list;
	};

	chooseCountry(doc) {
		if (doc) {
			let european = (doc.buyers || []).filter(buyer => buyer.buyerType === 'EUROPEAN_AGENCY');
			if (european.length > 0) {
				return 'EU';
			}
			return doc.country;
		}
		return undefined;
	}

	chooseTenderDate(doc) {
		let dates = [doc.awardDecisionDate, doc.awardDeadline, doc.contractSignatureDate];
		if (doc.lots) {
			doc.lots.forEach(lot => {
				if (lot.awardDecisionDate && dates.indexOf(lot.awardDecisionDate) < 0) {
					dates.push(lot.awardDecisionDate);
				}
			});
		}
		dates = dates.filter(date => {
			if (date !== null && date !== undefined) {
				let year = parseInt(date.slice(0, 4), 10);
				return (Utils.isValidDigiwhistYear(year));
			}
			return false;
		}).sort();
		return dates[0];
	}

	getMappedCPV(cpv) {
		let mapped = this.cpv_map[cpv.slice(0, 8)];
		if (mapped) {
			return mapped;
		}
		return cpv;
	}

	cleanCPV(cpv) {
		if (!cpv) {
			return undefined;
		}
		let result = ((cpv.split(' ')[0] || '').split(',')[0] || '').trim();
		if (cpv.indexOf('.') > 0) {
			//check if it#s dotted dkpp
			let mapped = this.getMappedCPV(result);
			if (mapped !== result) {
				if (this.library.isKnownCPV(mapped)) {
					return result;
				}
			}
		}
		//check if it's a standard XXXXXXXX-Y format
		result = result.replace(/\./g, '');
		if (result.indexOf('-') > 0) {
			let parts = result.split('-');
			let valid = (parts[0].length !== 8) || isNaN(parseInt(parts[0], 10)) || (parts[1].length !== 1) || isNaN(parseInt(parts[1], 10));
			if (valid) {
				if (this.stats) {
					this.stats.cpvs.count++;
				}
				result = this.getMappedCPV(result);
				if (this.library.isKnownCPV(result)) {
					return result.slice(0, 8);
				}
			}
		}

		// check if it's a number only XXXXXXXX format and maybe a short version XX00000
		let test = parseInt(result, 10);
		if (!isNaN(test)) {
			if ((result.length > 1) && (result.length < 6)) {
				//only fill short version if it's a valid short version
				while (result.length < 8) {
					result += '0';
				}
			}
			result = result.slice(0, 8);
			result = this.getMappedCPV(result);
			if (this.library.isKnownCPV(result)) {
				return result.slice(0, 8);
			}
		}
		return undefined;
	}

	cleanCPVS(list) {
		list = this.cleanList(list);
		if (list) {
			list = list.filter(cpv => {
				return cpv.code;
			});
			if (list.length === 0) {
				return undefined;
			}
		}
		return list;
	};

	chooseMainCVP(doc) {
		let cpv = doc.cpvs ? doc.cpvs.find(c => c.isMain) : null;
		if (cpv) {
			let result = this.cleanCPV(cpv.code);
			if (result) {
				if (this.stats) {
					this.stats.cpvs.count++;
				}
				return result;
			} else {
				if (this.stats) {
					this.stats.cpvs.unknown[cpv.code] = (this.stats.cpvs.unknown[cpv.code] || 0) + 1;
				}
			}
		}
		return undefined;
	}

	cleanItem(doc) {
		this.cleanProperties(['createdBy', 'createdByVersion', 'modifiedBy', 'modifiedByVersion'], doc);
		doc.lots = this.cleanLots(doc.lots);
		doc.specificationsProvider = this.cleanBody(doc.specificationsProvider);
		doc.furtherInformationProvider = this.cleanBody(doc.furtherInformationProvider);
		doc.bidsRecipient = this.cleanBody(doc.bidsRecipient);
		doc.buyers = this.cleanBodies(doc.buyers);
		doc.administrators = this.cleanBodies(doc.administrators);
		doc.onBehalfOf = this.cleanBodies(doc.onBehalfOf);
		doc.documents = this.cleanList(doc.documents);
		doc.publications = this.cleanPublications(doc.publications);
		doc.awardCriteria = this.cleanList(doc.awardCriteria);
		doc.cpvs = this.cleanCPVS(doc.cpvs);
		doc.fundings = this.cleanList(doc.fundings);
		doc.documentsPrice = this.cleanPrice(doc.documentsPrice);
		doc.estimatedPrice = this.cleanPrice(doc.estimatedPrice);
		doc.finalPrice = this.cleanPrice(doc.finalPrice);
		doc.indicators = this.cleanIndicators(doc.indicators);
		doc.documentsLocation = this.cleanAddress(doc.documentsLocation);
		doc.ot = {
			country: this.chooseCountry(doc),
			indicators: this.getIndicatorsAsFields(doc.indicators),
			scores: this.calculateIndicatorScores(doc.indicators),
			cpv: this.chooseMainCVP(doc),
			date: this.chooseTenderDate(doc)
		};
		this.shortenProperties(['buyerAssignedId'], doc, 200);
		return JSON.parse(JSON.stringify(doc)); //remove "undefined" properties
	};

	transform(items) {
		return items.map(item => {
			return this.cleanItem(item);
		});
	};
}

module.exports = Converter;
