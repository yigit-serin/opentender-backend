let getCountryCode = item => {
	if (item['modifiedBy'] !== 'eu.digiwhist.worker.eu.master.TedTenderMaster') {
		console.log('TODO parse countrycode from app string:', item['modifiedBy']);
	} else if (item && item.buyers && item.buyers.length > 0) {
		for (let i = 0; i < item.buyers.length; i++) {
			if (item.buyers[i].address && item.buyers[i].address.country) {
				return item.buyers[i].address.country;
			}
		}
	}
	return '?';
};

let cleanRecursive = (o, level) => {
	if (o === null) {
		return;
	}
	if (typeof o !== 'object') {
		return;
	}
	if (Array.isArray(o)) {
		return o.forEach(sub => cleanRecursive(sub, level + 1));
	}
	let removeFields = ['@class', '_id', 'rawObjectId', 'parsedObjectId', 'modifiedBy', 'createdBy', 'createdByVersion', 'modifiedByVersion', 'sourceLotIds', 'sourceBidIds', 'bodyIds'];
	let removeSubFields = ['created', 'modified'];

	removeFields.forEach(s => {
		if (o[s]) {
			delete o[s];
		}
	});
	if (o['buyerAssignedId']) {
		if (o['buyerAssignedId'].length > 200) {
			o['buyerAssignedId'] = o['buyerAssignedId'].slice(0, 200);
		}
	}
	if (o['bidId']) {
		o['id'] = o['bidId'];
		delete o['bidId'];
	}
	if (o['lotId']) {
		o['id'] = o['lotId'];
		delete o['lotId'];
	}
	if (o['groupId']) {
		o['groupId'] = cleanGroupID(o['groupId']);
	}
	if (o['finalPrice']) {
		if (o['finalPrice']['netAmount'] > 1000000000000) { //ignore prices larger than 1 trillion
			// console.log('Ignored finalPrice', o['finalPrice']['netAmount']);
			delete o['finalPrice'];
		}
	}
	if (level === 0) {
		if (o['groupId']) {
			o['id'] = o['groupId'];
			delete o['groupId'];
		}
	} else {
		removeSubFields.forEach(s => {
			if (o[s]) {
				delete o[s];
			}
		});
	}
	Object.keys(o).forEach(key => {
		cleanRecursive(o[key], level + 1);
	});
};

let cleanGroupID = (id) => {
	if (id.indexOf('group_') === 0) {
		return id.slice(6);
	}
	console.log('ALARM', 'what about the freakin groupId', id);
	return id;
};

let cleanItem = item => {
	cleanRecursive(item, 0);
	if (item.fundings && item.fundings.length === 0) {
		item.fundings = undefined;
	}
	if (item.lots) {
		item.lots.forEach(lot => {
			if (lot.bids) {
				lot.bids.forEach(bid => {
					if (bid.unitPrices) {
						bid.unitPrices = bid.unitPrices.filter(up => {
							return Object.keys(up).length > 0;
						});
					}
					if (item.unitPrices && item.unitPrices.length === 0) {
						item.unitPrices = undefined;
					}
				});
			}
		});
	}
};

let cleanItems = items => {
	items.forEach(item => {
		item.country = getCountryCode(item);
		cleanItem(item);
	});
};


// let doc = yaml.safeLoad(fs.readFileSync(path.resolve(__dirname, 'fingerprints.yaml'), 'utf8'));
// let regexes = [];
// let quote = function (str) {
// 	return '\\b' + str.replace(/([.?*+^$[\]\\(){}|-])/g, "\\$1") + '\\b';
// };
// doc.person_prefix.forEach(p => {
// 	regexes.push(new RegExp(quote(p), 'ig'));
// });
// Object.keys(doc.company_types).forEach(key => {
// 	let list = doc.company_types[key];
// 	regexes.push(new RegExp(quote(key), 'ig'));
// 	list.forEach(p => {
// 		regexes.push(new RegExp(quote(p), 'ig'));
// 	});
// });
//
// let slug = (full) => {
// 	let s = (full || '').toLowerCase().split(',')[0].split('(')[0];
// 	regexes.forEach(r => {
// 		s = s.replace(r, '');
// 	});
// 	s = s.replace(/\d*$/, '');
// 	// console.log(full, '=>', s);
// 	return s;
// };

// let fillfingerprints = (items, cb) => {
// items.forEach(item => {
// 	item.body.fingerprint = slug(item.body.name);
// });
// 	cb();
// };

module.exports = {
	cleanTenderApiDoc: cleanItem,
	cleanTenderApiDocs: cleanItems
};
