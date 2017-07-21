module.exports = {

	isValidDigiwhistYear: (year) => {
		//TODO: make sure dates are validated on import & make valid date range a config entry
		return (year >= 2009 && year < 2018);
	}

};