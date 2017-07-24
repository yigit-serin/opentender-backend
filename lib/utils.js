module.exports = {

	isValidDigiwhistYear: (year) => {
		//TODO: make sure dates are validated on import & make valid date range a config entry
		return (year >= 2009 && year < 2018);
	},

	validateId: (id) => {
		if ((typeof id !== 'string') || ( id.trim() === '')) return null;
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
	}

};