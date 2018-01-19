let nested_objects = ['lots.bids.bidders', 'lots.bids', 'lots.cpvs', 'lots', 'buyers', 'cpvs', 'indicators', 'ot.scores'];

module.exports = {

	isValidDigiwhistYear: (year) => {
		//TODO: make sure dates are validated on import & make valid date range a config entry
		return (year >= 2009 && year <= 2018);
	},

	isValidCentury: (year) => {
		return (year >= 2000 && year < 2100);
	},

	validateId: (id) => {
		if ((typeof id !== 'string') || (id.trim() === '')) return null;
		id = id.trim();
		if (id.length === 0) return null;
		return id;
	},

	validateIds: (ids) => {
		if (!Array.isArray(ids)) ids = [ids];
		ids = ids.filter(id => {
			return (typeof id === 'string') && (id.trim().length > 0);
		});
		if (ids.length === 0) return null;
		return ids;
	},

	getNestedField: (fieldname) => {
		return nested_objects.find(nested => {
			return fieldname.indexOf(nested) === 0;
		});
	},

	clone: (o) => {
		return JSON.parse(JSON.stringify(o));
	},

	roundValueTwoDecimals: (value) => {
		if (value === null || value === undefined) {
			return value;
		}
		return Math.round(value * 100) / 100;
	},

	roundValueFourDecimals: (value) => {
		if (value === null || value === undefined) {
			return value;
		}
		return Math.round(value * 10000) / 10000;
	}

};
