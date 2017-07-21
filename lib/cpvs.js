const fs = require('fs');

const CPVs = function (config) {

	let cpvs = {};
	let cpvs_main = {};

	let cpv_list = JSON.parse(fs.readFileSync(config.data.path + '/cpvs.json').toString());

	cpv_list.forEach(cpv => {
		let s = cpv.CODE.slice(0, 8);
		cpvs[s] = cpv;
		if (cpv.CODE.slice(2, 8) === '000000') {
			s = cpv.CODE.slice(0, 2);
			cpvs_main[s] = cpv;
		}
	});

	this.getCPVName = (id, lang) => {
		let cpv = cpvs_main[id];
		if (!cpv) {
			cpv = cpvs[id];
		}
		if (cpv) {
			return this.getName(cpv, lang);
		}
		return 'CPV#' + id;
	};

	this.getName = (cpv, lang) => {
		let result = cpv[lang];
		if (!result) result = cpv['EN'];
		return result;
	};

	this.parseCPVs = (id) => {
		let cpv = cpvs_main[id];
		if (!cpv) {
			cpv = cpvs[id];
		}
		if (!cpv) {
			return {};
		}
		let parent_cpv = id.length > 2 ? cpvs_main[id.slice(0, 2)] : null;
		return {
			cpv, parent_cpv
		};
	};

};


module.exports = CPVs;