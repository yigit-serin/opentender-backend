const path = require('path');
const fs = require('fs');

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

class Converter {

	constructor(stats, library, datapath) {
		this.stats = stats;
		this.library = library;
		this.pc2NUTS = loadPC2NUTS(datapath);
		this.nuts_maps = loadNUTSmaps(datapath);
		if (this.stats) {
			this.stats.nuts = this.stats.nuts || {count: 0, mappedByPostCode: 0, unknown: {}, known: {}, mapped: {}};
			this.stats.body = this.stats.body || {count: 0, noname: 0};
		}
	}

	cleanNUTS(nuts) {
		let result = (nuts || []).filter(nut => nut !== null).map(n => {
			let nut = n.split('-')[0].trim();
			let map = this.nuts_maps.find(m => nut === m.src);
			if (map) {
				if (this.stats) {
					this.stats.nuts.mapped[nut] = (this.stats.nuts.mapped[nut] || 0) + 1;
				}
				nut = map.dest;
			}
			if (this.stats) {
				this.stats.nuts.count = this.stats.nuts.count + 1;
				if (!this.library.isKnownNUTSCode(nut)) {
					this.stats.nuts.unknown[nut] = (this.stats.nuts.unknown[nut] || 0) + 1;
				} else {
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

	checkNUTS(doc) {
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
				if (code) {
					this.stats.nuts.mappedByPostCode = this.stats.nuts.mappedByPostCode + 1;
					doc.nuts = [code];
					// } else {
					// console.log(postcode, doc.nuts, JSON.stringify(doc));
				}
			}
		}
		doc.nuts = this.cleanNUTS(doc.nuts);
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

	cleanIndicators(list) {
		list = this.cleanList(list);
		if (list) {
			list.forEach(doc => {
				if (!isNaN(doc.value)) {
					if (doc.type.indexOf('TRANSPARENCY') === 0 || doc.type.indexOf('CORRUPTION') === 0) {
						doc.value = 1 - doc.value;
					}
					doc.value = doc.value * 100;
				}
				this.cleanProperties(['@class', 'id', 'relatedEntityId', 'created', 'modified', 'createdBy', 'modifiedBy', 'createdByVersion', 'modifiedByVersion', 'metaData'], doc);
			});
		}
		return list;
	};

	calculateIndicatorScores(list) {
		let result = [];
		let indicators = {
			CORRUPTION: [],
			TRANSPARENCY: [],
			ADMINISTRATIVE: []
		};
		if (list) {
			list.forEach(doc => {
				if (doc.status === 'CALCULATED' && !isNaN(doc.value)) {
					Object.keys(indicators).forEach(key => {
						if (doc.type.indexOf(key) === 0) {
							indicators[key].push(doc);
						}
					});
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
				doc.fundings = this.cleanList(doc.fundings);
				doc.bids = this.cleanBids(doc.bids);
				doc.estimatedPrice = this.cleanPrice(doc.estimatedPrice);
			});
		}
		return list;
	};


	cleanAddress(doc) {
		if (doc) {
			this.checkNUTS(doc);
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
		this.cleanProperties(['publicationDate'], doc);
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

	remapEuropeanCommunityCountry(doc) {
		let european = (doc.buyers || []).filter(buyer => buyer.buyerType === 'EUROPEAN_AGENCY');
		if (european.length > 0) {
			doc.country = 'EU';
		}
	}

	chooseTenderData(doc) {

	}

	cleanItem(doc) {
		this.cleanProperties(['createdBy', 'createdByVersion', 'modifiedBy', 'modifiedByVersion'], doc);
		this.remapEuropeanCommunityCountry(doc);
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
		doc.cpvs = this.cleanList(doc.cpvs);
		doc.fundings = this.cleanList(doc.fundings);
		doc.documentsPrice = this.cleanPrice(doc.documentsPrice);
		doc.estimatedPrice = this.cleanPrice(doc.estimatedPrice);
		doc.finalPrice = this.cleanPrice(doc.finalPrice);
		doc.indicators = this.cleanIndicators(doc.indicators);
		doc.scores = this.calculateIndicatorScores(doc.indicators);
		doc.data = this.chooseTenderData(doc);
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
