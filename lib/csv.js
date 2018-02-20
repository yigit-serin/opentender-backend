const moment = require('moment');

const BOOLSTR = {
	TRUE: 'true',
	FALSE: 'false'
};
const lineDelimiter = '\n';
const colDelimiter = ';';
const escapeNewlines = true;
const escapeRegexp = new RegExp('[' + colDelimiter + '\r\n"]');

/*

TODO: does it make sense to include to csv?
tender.document
tender.furtherInformationProvider
tender.bidsRecipient
tender.addressOfImplementation
tender.documentsLocation
tender.corrections
 */

/*
	found in Datlabs csv, fields unknown
	recorded_bids_count
	bid_final_price
	buyer_master_id
	bidder_master_id
	award_url,
	award_date,
	year
	savings
	award_period_length
 */

let TenderColumns = [
	{type: 'nr'},
	{field: 'id', type: 'string'},
	{field: 'country', type: 'string'},
	{field: 'title', type: 'string'},
	{field: 'size', type: 'string'},
	{field: 'supplyType', type: 'string'},
	{field: 'procedureType', type: 'string'},
	{field: 'nationalProcedureType', type: 'string'},
	{field: 'selectionMethod', type: 'string'},
	{field: 'appealBodyName', type: 'string'},
	{field: 'mediationBodyName', type: 'string'},

	{
		name: 'mainCpv', transform: (tender) => {
			let result = (tender.cpvs || []).find(cpv => {
				return cpv.isMain;
			});
			if (result) {
				return result.code
			}
			return '';
		}
	},
	{
		name: 'cpvs', transform: (tender) => {
			let result = (tender.cpvs || []).map(cpv => {
				return cpv.code;
			});
			return result.join(',');
		}
	},

	{
		name: 'fundingProgrammes', transform: (tender) => {
			let result = (tender.fundings || []).map(funding => {
				return funding.programme;
			}).filter(s => s && s.length > 0);
			return result.join(',');
		}
	},

	{field: 'eligibleBidLanguages', type: 'strings'},
	{field: 'npwp_reasons', type: 'strings'},

	{field: 'maxFrameworkAgreementParticipants', type: 'number'},
	{field: 'maxBidsCount', type: 'number'},
	{field: 'envisagedMinCandidatesCount', type: 'number'},
	{field: 'envisagedMaxCandidatesCount', type: 'number'},
	{field: 'envisagedCandidatesCount', type: 'number'},

	{field: 'awardDeadline', type: 'date'},
	{field: 'contractSignatureDate', type: 'date'},
	{field: 'awardDecisionDate', type: 'date'},
	{field: 'bidDeadline', type: 'date'},
	{field: 'documentsDeadline', type: 'date'},
	{field: 'completionDate', type: 'date'},
	{field: 'cancellationDate', type: 'date'},
	{field: 'contractSignatureDate', type: 'date'},
	{field: 'estimatedStartDate', type: 'date'},
	{field: 'estimatedCompletionDate', type: 'date'},
	{field: 'estimatedDurationInYears', type: 'number'},
	{field: 'estimatedDurationInMonths', type: 'number'},
	{field: 'estimatedDurationInDays', type: 'number'},

	{
		name: 'isEUFunded', transform: (tender) => {
			if (!tender.fundings || tender.fundings.length === 0) {
				return '';
			}
			let result = (tender.fundings || []).find(fund => {
				return fund.isEuFund;
			});
			return result ? BOOLSTR.TRUE : BOOLSTR.FALSE;
		}
	},
	{field: 'isDps', type: 'bool'},
	{field: 'isElectronicAuction', type: 'bool'},
	{field: 'isEInvoiceAccepted', type: 'bool'},
	{field: 'isAwarded', type: 'bool'},
	{field: 'isCentralProcurement', type: 'bool'},
	{field: 'isJointProcurement', type: 'bool'},
	{field: 'isOnBehalfOf', type: 'bool'},
	{field: 'isFrameworkAgreement', type: 'bool'},
	{field: 'isCoveredByGpa', type: 'bool'},
	{field: 'isAcceleratedProcedure', type: 'bool'},
	{field: 'isWholeTenderCancelled', type: 'bool'},
	{field: 'documentsPayable', type: 'bool'},
	{field: 'hasLots', type: 'bool'},
	{field: 'areVariantsAccepted', type: 'bool'},
	{field: 'isDocumentsAccessRestricted', type: 'bool'},
	{field: 'hasOptions', type: 'bool'},

	{field: 'documentsPrice', type: 'price'},
	{field: 'estimatedPrice', type: 'price'},
	{field: 'finalPrice', type: 'price'},

	{field: 'description', type: 'string'},
	{field: 'eligibilityCriteria', type: 'string'},
	{field: 'personalRequirements', type: 'string'},
	{field: 'economicRequirements', type: 'string'},
	{field: 'technicalRequirements', type: 'string'},
	{field: 'cancellationReason', type: 'string'},
	{field: 'modificationReason', type: 'string'},
	{field: 'excessiveFrameworkAgreementJustification', type: 'string'},
	{field: 'acceleratedProcedureJustification', type: 'string'},
	{field: 'modificationReasonDescription', type: 'string'},
	{field: 'deposits', type: 'string'},

	{field: 'documents', type: 'count'},
	{field: 'awardCriteria', type: 'count'},
	{field: 'corrections', type: 'count'},

	{field: 'created', type: 'date'},
	{field: 'modified', type: 'date'},
];

let LotColumns = [
	{type: 'nr'},
	{field: 'title', type: 'string'},
	{field: 'selectionMethod', type: 'string'},
	{field: 'contractNumber', type: 'string'},
	{field: 'status', type: 'string'},
	{
		name: 'mainCpv', transform: (lot) => {
			let result = (lot.cpvs || []).find(cpv => {
				return cpv.isMain;
			});
			if (result) {
				return result.code
			}
			return '';
		}
	},
	{
		name: 'cpvs', transform: (lot) => {
			let result = (lot.cpvs || []).map(cpv => {
				return cpv.code;
			});
			return result.join(',');
		}
	},

	{field: 'awardDecisionDate', type: 'date'},
	{field: 'estimatedCompletionDate', type: 'date'},
	{field: 'estimatedStartDate', type: 'date'},
	{field: 'contractSignatureDate', type: 'date'},
	{field: 'cancellationDate', type: 'date'},
	{field: 'completionDate', type: 'date'},

	{field: 'isAwarded', type: 'bool'},
	{field: 'isAwardedToGroupOfSuppliers', type: 'bool'},

	{field: 'estimatedPrice', type: 'price'},

	{field: 'lotNumber', type: 'number'},
	{field: 'bidsCount', type: 'number'},
	{field: 'validBidsCount', type: 'number'},
	{field: 'smeBidsCount', type: 'number'},
	{field: 'electronicBidsCount', type: 'number'},
	{field: 'nonEuMemberStatesCompaniesBidsCount', type: 'number'},
	{field: 'otherEuMemberStatesCompaniesBidsCount', type: 'number'},
	{field: 'foreignCompaniesBidsCount', type: 'number'},
	{field: 'estimatedDurationInMonths', type: 'number'},
	{field: 'estimatedDurationInDays', type: 'number'},
	{field: 'estimatedDurationInYears', type: 'number'},
	{field: 'maxFrameworkAgreementParticipants', type: 'number'},

	{field: 'description', type: 'string'},
	{field: 'economicRequirements', type: 'string'},
	{field: 'personalRequirements', type: 'string'},
	{field: 'technicalRequirements', type: 'string'},
	{field: 'eligibilityCriteria', type: 'string'},
	{field: 'cancellationReason', type: 'string'},

	{
		name: 'fundingProgrammes', transform: (lot) => {
			let result = (lot.fundings || []).map(funding => {
				return funding.programme;
			}).filter(s => s && s.length > 0);
			return result.join(',');
		}
	}
];

let BidColumns = [
	{type: 'nr'},

	{field: 'isWinning', type: 'bool'},
	{field: 'isSubcontracted', type: 'bool'},
	{field: 'isConsortium', type: 'bool'},

	{field: 'subcontractedProportion', type: 'number'},
	{field: 'monthlyPriceMonthsCount', type: 'number'},
	{field: 'annualPriceYearsCount', type: 'number'},

	{field: 'price', type: 'price'},
	{field: 'subcontractedValue', type: 'price'},

	{field: 'bidders', type: 'count'},
	{field: 'subcontractors', type: 'count'},
];

let BodyColumns = [
	{field: 'id', type: 'string'},
	{field: 'name', type: 'string'},
	{field: 'email', type: 'string'},
	{field: 'phone', type: 'string'},
	{field: 'web', type: 'string'},
	{field: 'contactName', type: 'string'},
	{field: 'contactPoint', type: 'string'},
	{
		name: 'nuts', transform: (body) => {
			if (!body.address || !body.address.nuts || body.address.nuts.length === 0) return '';
			return body.address.nuts.join(',');
		}
	},
	{
		name: 'city', transform: (body) => {
			if (!body.address || !body.address.city) return '';
			return body.address.city;
		}
	},
	{
		name: 'country', transform: (body) => {
			if (!body.address || !body.address.country) return '';
			return body.address.country;
		}
	},
	{
		name: 'postcode', transform: (body) => {
			if (!body.address || !body.address.postcode) return '';
			return body.address.postcode;
		}
	}
];

let BodiesColumns = [
	{type: 'nr'},
].concat(BodyColumns);

let BuyerColumns = [
	{type: 'nr'},
	{field: 'buyerType', type: 'string'},
	{field: 'mainActivities', type: 'strings'},
].concat(BodyColumns);

let BidderColumns = [
	{type: 'nr'},
	{field: 'isLeader', type: 'bool'}
].concat(BodyColumns);

let PublicationColumns = [
	{type: 'nr'},
	{field: 'source', type: 'string'},
	{field: 'humanReadableUrl', type: 'string'},
	{field: 'machineReadableUrl', type: 'string'},
	{field: 'language', type: 'string'},
	{field: 'dispatchDate', type: 'date'},
	{field: 'publicationDate', type: 'date'}
];

let columnToHead = (column, prefix) => {
	if (column.transform) {
		return prefix + '_' + column.name;
	}
	if (column.type === 'nr') {
		return prefix + '_row_nr';
	}
	if (column.type === 'count') {
		return prefix + '_' + column.field + '_count';
	}
	if (column.type === 'length') {
		return prefix + '_' + column.field + '_length';
	}
	if (column.type === 'indicator') {
		return prefix + '_indicator_' + column.field;
	}
	if (!column.field) {
		console.log('unknown column type', column);
	}
	return prefix + '_' + column.field;
};

let columnToString = (obj, column, nr) => {
	if (column.transform) {
		return column.transform(obj);
	}
	if (column.type === 'nr') {
		return (nr + 1).toString();
	}
	if (column.type === 'indicator') {
		if (obj.indicators === undefined) return '';
		let indicator = obj.indicators.find(ind => ind.type === column.field);
		if (!indicator) return '';
		if (indicator.value === undefined) return '';
		return indicator.value.toString();
	}

	let value = obj[column.field];
	if (value === undefined) return '';

	if (column.type === 'string') {
		return value || '';
	}
	if (column.type === 'strings') {
		return (value || []).join(',');
	}
	if (column.type === 'bool') {
		return value ? BOOLSTR.TRUE : BOOLSTR.FALSE;
	}
	if (column.type === 'price') {
		if (value.netAmountEur === undefined) return '';
		return value.netAmountEur.toString();
	}
	if (column.type === 'date') {
		return moment(value).format('YYYY/MM/DD');
	}
	if (column.type === 'number') {
		return value.toString();
	}
	if (column.type === 'count') {
		return value.length.toString();
	}
	if (column.type === 'length') {
		return value.length.toString();
	}
	console.log('unknown column', column);
	return '';
};

let emptyCols = (count) => {
	let result = [];
	for (let i = 0; i < count; i++) result.push('');
	return result;
};

let processLine = (obj, cols, nr) => {
	return cols.map(column => {
		return columnToString(obj, column, nr);
	});
};

class Block {
	constructor(columnBlocks, parentName, prefix, parentIsObject) {
		this.columnBlocks = columnBlocks;
		this.parentName = parentName;
		this.parentIsObject = parentIsObject;
		this.prefix = prefix;
	}

	columnCount() {
		let result = 0;
		this.columnBlocks.forEach(c => {
			if (Array.isArray(c)) {
				result += c.length;
			} else {
				result += c.columnCount();
			}
		});
		return result;
	}

	header() {
		let result = [];
		this.columnBlocks.forEach(c => {
			if (Array.isArray(c)) {
				result = result.concat(c.map(column => {
					return columnToHead(column, this.prefix);
				}));
			} else {
				result = result.concat(c.header());
			}
		});
		return result;
	}

	process(obj, nr) {
		let blocks = [];
		this.columnBlocks.forEach(c => {
			if (Array.isArray(c)) {
				let block = {
					columns: c,
					lines: [
						processLine(obj, c, nr)
					]
				};
				blocks.push(block);
			} else {
				let block = {
					columns: c,
					lines: []
				};
				if (c.parentIsObject) {
					if (obj[c.parentName]) {
						block.lines = block.lines.concat(c.process(obj[c.parentName], 0));
					}
				} else {
					(obj[c.parentName] || []).forEach((o, index) => {
						block.lines = block.lines.concat(c.process(o, index));
					});
				}
				blocks.push(block);
			}
		});

		let countAfter = (index) => {
			let result = 0;
			for (let i = index + 1; i < this.columnBlocks.length; i++) {
				let c = this.columnBlocks[i];
				result += Array.isArray(c) ? c.length : c.columnCount();
			}
			return result;
		};

		let column_count = 0;
		let result = [];
		blocks.forEach((block, index) => {
			let column_before = column_count;
			let column_after = countAfter(index);
			if (block.lines.length === 1 && result.length > 0) {
				block.lines[0].forEach((col, index) => {
					result[0][column_count + index] = col;
				});
			} else {
				block.lines.forEach(line => {
					result.push(emptyCols(column_before).concat(line).concat(emptyCols(column_after)));
				});
			}
			column_count += Array.isArray(block.columns) ? block.columns.length : block.columns.columnCount();
		});
		return result;
	}
}

let filterCustomColums = (columns, headIds, prefix) => {
	if (!headIds) {
		return columns;
	}
	return columns.filter(col => {
		let col_name = columnToHead(col, prefix);
		return headIds.indexOf(col_name) >= 0;
	});
};

let buildTenderBlock = (headIds, library) => {
	let bidder_block = new Block([filterCustomColums(BidderColumns, headIds, 'bidder')], 'bidders', 'bidder');
	let bid_block = new Block([filterCustomColums(BidColumns, headIds, 'bid'), bidder_block], 'bids', 'bid');
	let lot_block = new Block([filterCustomColums(LotColumns, headIds, 'lot'), bid_block], 'lots', 'lot');
	let buyer_block = new Block([filterCustomColums(BuyerColumns, headIds, 'buyer')], 'buyers', 'buyer');
	let administrators_block = new Block([filterCustomColums(BodiesColumns, headIds, 'administrator')], 'administrators', 'administrator');
	let publication_block = new Block([filterCustomColums(PublicationColumns, headIds, 'publication')], 'publications', 'publication');
	let onbehalfof_block = new Block([filterCustomColums(BodiesColumns, headIds, 'onBehalfOf')], 'onBehalfOf', 'onBehalfOf');
	let indicatorColumns = library.schema.definitions['indicator-type'].enum.map(id => {
		return {
			field: id, type: 'indicator'
		}
	});
	return new Block([filterCustomColums(TenderColumns.concat(indicatorColumns), headIds, 'tender'), lot_block, buyer_block, administrators_block, onbehalfof_block, publication_block], '', 'tender');
};

const escapeCSV = (str) => {
	if (typeof(str) === 'string') {
		if (escapeNewlines) {
			str = str.replace(/\n/g, '\\n')
		}
		str = escapeRegexp.test(str) ? '"' + str.replace(/"/g, '""') + '"' : str
	} else {
		console.log('warning, ', str, 'not a string');
	}
	return str;
};

class CSV {

	constructor(library, headIds) {
		this.tenderCSV = buildTenderBlock(headIds, library);
		this.library = library;
	}

	header() {
		return this.tenderCSV.header().map(col => {
			return escapeCSV(col);
		}).join(colDelimiter) + lineDelimiter;
	}

	headIds() {
		return this.tenderCSV.header();
	}

	transform(tender, nr) {
		let rows = this.tenderCSV.process(tender, nr);
		let tender_id_col = this.headIds().indexOf('tender_id');
		return rows.map(row => {
			if (tender_id_col >= 0) {
				row[tender_id_col] = tender.id;
			}
			return row.map((col, index) => {
				return escapeCSV(col);
			}).join(colDelimiter);
		}).join(lineDelimiter) + lineDelimiter;
	}

}


module.exports = CSV;
