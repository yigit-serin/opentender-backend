// https://en.wikipedia.org/wiki/NUTS_statistical_regions_of_Greece
let NUTSMapping = [
	{src: 'GR1', dest: 'EL5'},
	{src: 'GR2', dest: 'EL6'},
	{src: 'GR3', dest: 'EL7'},
	{src: 'GR4', dest: 'EL4'}
];

let cleanNUTS = (nuts, stats) => {
	let result = (nuts || []).filter(nut => nut !== null).map(nut => {
		nut = nut.split('-')[0].trim();
		let map = NUTSMapping.find(m => nut.indexOf(m.src) === 0);
		if (map) {
			if (stats) {
				stats.greece = stats.greece || {};
				stats.greece[nut] = (stats.greece[nut] || 0) + 1;
			}
			nut = map.dest + nut.slice(map.src.length);
		}
		if (stats) {
			stats.nuts_count = (stats.nuts_count || 0) + 1;
			if (!nuts_names[nut]) {
				stats.invalid_nuts = stats.invalid_nuts || {};
				stats.invalid_nuts[nut] = (stats.invalid_nuts[nut] || 0) + 1;
			} else {
				stats.valid_nuts = stats.valid_nuts || {};
				stats.valid_nuts[nut] = (stats.valid_nuts[nut] || 0) + 1;
			}
		}
		return nut;
	}).filter(nut => nut.length > 0);
	if (result.length === 0) {
		return undefined;
	}
	return result;
};

let shortenProperties = (names, o, amount) => {
	names.forEach(s => {
		if (o[s] && o[s].length > amount) {
			o[s] = o[s].slice(0, amount);
		}
	});
};

let cleanProperties = (names, o) => {
	names.forEach(s => {
		if (o[s]) {
			o[s] = undefined;
		}
	});
};

let cleanGroupID = (id) => {
	if (id.indexOf('group_') === 0) {
		return id.slice(6);
	}
	console.log('ALARM', 'what about the freakin groupId', id);
	return id;
};

let cleanList = (list) => {
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

let cleanIndicators = (list) => {
	list = cleanList(list);
	if (list) {
		list.forEach(doc => {
			cleanProperties(['_id', 'id', 'relatedEntityId', 'created', 'modified', 'createdBy', 'modifiedBy', 'createdByVersion', 'modifiedByVersion'], doc);
		});
	}
	return list;
};

let cleanBids = (list, stats) => {
	list = cleanList(list);
	if (list) {
		list.forEach(doc => {
			cleanProperties(['sourceBidIds'], doc);
			doc.bidders = cleanBodies(doc.bidders, stats);
			doc.unitPrices = cleanList(doc.unitPrices);
			doc.subcontractedValue = cleanPrice(doc.subcontractedValue, stats);
			doc.price = cleanPrice(doc.price, stats);
		});
	}
	return list;
};

let cleanLots = (list, stats) => {
	list = cleanList(list);
	if (list) {
		list.forEach(doc => {
			if (doc.title === doc.description) {
				doc.description = undefined;
			}
			cleanProperties(['sourceLotIds'], doc);
			doc.fundings = cleanList(doc.fundings);
			doc.bids = cleanBids(doc.bids, stats);
			doc.estimatedPrice = cleanPrice(doc.estimatedPrice, stats);
		});
	}
	return list;
};

let cleanAddress = (doc, stats) => {
	if (doc) {
		doc.nuts = cleanNUTS(doc.nuts, stats);
	}
};

let cleanPublications = (list) => {
	list = cleanList(list);
	if (list) {
		list.forEach(doc => {
			cleanProperties(['@class'], doc);
			shortenProperties(['buyerAssignedId'], doc, 200);
		});
	}
	return list;
};

let cleanPrice = (doc, stats) => {
	if (!doc) {
		return undefined;
	}
	if ((doc['netAmountEUR'] > 1000000000000)) { //ignore prices larger than 1 trillion
		console.log(doc);
		if (stats) {
			stats.ignored_prices = stats.ignored_prices || [];
			stats.ignored_prices.push(doc['netAmountEUR']);
		}
		return undefined;
	}
	return doc;
};

let cleanBody = (doc, stats) => {
	if (!doc) {
		return undefined;
	}
	doc.id = cleanGroupID(doc.groupId);
	cleanProperties(['groupId', 'created', 'modified', 'createdBy', 'createdByVersion', 'modifiedBy', 'modifiedByVersion', '_id', 'bodyIds'], doc);
	cleanAddress(doc.address, stats);
};

let cleanBodies = (list, stats) => {
	list = cleanList(list);
	if (list) {
		list.forEach(doc => {
			cleanBody(doc, stats);
		});
	}
	return list;
};

let cleanItem = (doc, stats) => {
	doc.id = cleanGroupID(doc.groupId);
	cleanProperties(['_id', 'groupId', 'createdBy', 'createdByVersion', 'modifiedBy', 'modifiedByVersion'], doc);
	doc.lots = cleanLots(doc.lots, stats);
	doc.specificationsProvider = cleanBody(doc.specificationsProvider, stats);
	doc.furtherInformationProvider = cleanBody(doc.furtherInformationProvider, stats);
	doc.bidsRecipient = cleanBody(doc.bidsRecipient, stats);
	doc.buyers = cleanBodies(doc.buyers, stats);
	doc.administrators = cleanBodies(doc.administrators, stats);
	doc.onBehalfOf = cleanBodies(doc.onBehalfOf, stats);
	doc.documents = cleanList(doc.documents);
	doc.publications = cleanPublications(doc.publications);
	doc.awardCriteria = cleanList(doc.awardCriteria);
	doc.cpvs = cleanList(doc.cpvs);
	doc.fundings = cleanList(doc.fundings);
	doc.indicators = cleanIndicators(doc.indicators);
	doc.estimatedPrice = cleanPrice(doc.estimatedPrice, stats);
	doc.finalPrice = cleanPrice(doc.finalPrice, stats);
	return JSON.parse(JSON.stringify(doc)); //remove "undefined" properties
};

let cleanItems = (items, stats) => {
	return items.map(item => {
		return cleanItem(item, stats);
	});
};

module.exports = {
	cleanTenderApiDocs: cleanItems
};
