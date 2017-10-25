// https://en.wikipedia.org/wiki/NUTS_statistical_regions_of_Greece
let NUTSMapping = [
	{src: 'GR1', dest: 'EL5'},
	{src: 'GR2', dest: 'EL6'},
	{src: 'GR3', dest: 'EL7'},
	{src: 'GR4', dest: 'EL4'}
];

class TenderConverter {

	constructor(stats, library) {
		this.stats = stats;
		this.library = library;
	}

	cleanNUTS(nuts) {
		let result = (nuts || []).filter(nut => nut !== null).map(nut => {
			nut = nut.split('-')[0].trim();
			let map = NUTSMapping.find(m => nut.indexOf(m.src) === 0);
			if (map) {
				if (this.stats) {
					this.stats.greece = this.stats.greece || {};
					this.stats.greece[nut] = (this.stats.greece[nut] || 0) + 1;
				}
				nut = map.dest + nut.slice(map.src.length);
			}
			if (this.stats) {
				this.stats.nuts_count = (this.stats.nuts_count || 0) + 1;
				if (!this.library.isKnownNUTSCode(nut)) {
					this.stats.invalid_nuts = this.stats.invalid_nuts || {};
					this.stats.invalid_nuts[nut] = (this.stats.invalid_nuts[nut] || 0) + 1;
				} else {
					this.stats.valid_nuts = this.stats.valid_nuts || {};
					this.stats.valid_nuts[nut] = (this.stats.valid_nuts[nut] || 0) + 1;
				}
			}
			return nut;
		}).filter(nut => nut.length > 0);
		if (result.length === 0) {
			return undefined;
		}
		return result;
	};

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
				this.cleanProperties(['_id', 'id', 'relatedEntityId', 'created', 'modified', 'createdBy', 'modifiedBy', 'createdByVersion', 'modifiedByVersion'], doc);
			});
		}
		return list;
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
				doc.fundings = this.cleanList(doc.fundings);
				doc.bids = this.cleanBids(doc.bids);
				doc.estimatedPrice = this.cleanPrice(doc.estimatedPrice);
			});
		}
		return list;
	};


	cleanAddress(doc) {
		if (doc) {
			doc.nuts = this.cleanNUTS(doc.nuts);
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

	cleanItem(doc) {
		doc.id = this.cleanGroupID(doc.groupId);
		this.cleanProperties(['_id', 'groupId', 'createdBy', 'createdByVersion', 'modifiedBy', 'modifiedByVersion'], doc);
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
		doc.indicators = this.cleanIndicators(doc.indicators);
		doc.estimatedPrice = this.cleanPrice(doc.estimatedPrice);
		doc.finalPrice = this.cleanPrice(doc.finalPrice);
		return JSON.parse(JSON.stringify(doc)); //remove "undefined" properties
	};

	transform(items) {
		return items.map(item => {
			return this.cleanItem(item);
		});
	};
}

module.exports = TenderConverter;
