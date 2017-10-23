const fs = require('fs');

let cpvTypes = [
	{
		name: 'categories',
		length: 5
	},
	{
		name: 'groups',
		length: 3
	},
	{
		name: 'divisions',
		length: 2
	}
];

// https://en.wikipedia.org/wiki/NUTS_statistical_regions_of_Greece
let NUTSMapping = [
	{src: 'GR1', dest: 'EL5'},
	{src: 'GR2', dest: 'EL6'},
	{src: 'GR3', dest: 'EL7'},
	{src: 'GR4', dest: 'EL4'}
];

class Library {

	constructor(config) {

		this.cpvs_full = {};
		this.cpvs_divisions = {};
		this.cpvs_groups = {};
		this.cpvs_categories = {};

		this.cpv_list = JSON.parse(fs.readFileSync(config.data.path + '/cpvs.json').toString());
		this.nuts_names = JSON.parse(fs.readFileSync(config.data.shared + '/nuts/nuts_names.json').toString());

		this.cpv_list.forEach(cpv => {
			let s = cpv.CODE.slice(0, 8);
			this.cpvs_full[s] = cpv;
			if (cpv.CODE.slice(2, 8) === '000000') {
				s = cpv.CODE.slice(0, 2);
				this.cpvs_divisions[s] = cpv;
			}
			if (cpv.CODE.slice(3, 8) === '00000') {
				s = cpv.CODE.slice(0, 3);
				this.cpvs_groups[s] = cpv;
			}
			if (cpv.CODE.slice(5, 8) === '000') {
				s = cpv.CODE.slice(0, 5);
				this.cpvs_categories[s] = cpv;
			}
		});
	}

	getCPVName(id, lang) {
		let cpv = this.cpvs_divisions[id] || this.cpvs_groups[id] || this.cpvs_categories[id] || this.cpvs_full[id];
		if (cpv) {
			return Library.getCPVObjectName(cpv, lang);
		}
		return 'CPV#' + id;
	}

	static getCPVObjectName(cpv, lang) {
		let result = cpv[(lang || 'EN').toUpperCase()];
		if (!result) {
			result = cpv['EN'];
		}
		return result;
	}

	parseCPVs(id, lang) {
		let cpv_objects = [];
		let sublevel = null;
		cpvTypes.forEach((cpvtype, i) => {
			let cpvcode = id.slice(0, cpvtype.length);
			if (cpvcode.length === cpvtype.length) {
				let cpv = {
					id: cpvcode,
					level: cpvtype.name,
					name: this.getCPVName(cpvcode, lang)
				};
				if (cpv_objects.length === 0 && i > 0) {
					sublevel = cpvTypes[i - 1].name;
				}
				cpv_objects.push(cpv);
			}
		});
		return {
			sublevel,
			cpv: cpv_objects[0],
			parents: cpv_objects.slice(1)
		};
	}

	mapNut(id) {
		id = id.split('-')[0].trim();
		let nut = id.toUpperCase();
		let map = NUTSMapping.find(m => nut.indexOf(m.src) === 0);
		if (map) {
			console.log(nut, map, map.dest + nut.slice(map.src.length));
			return map.dest + nut.slice(map.src.length);
		}
		return id;
	}

	parseNUTS(id) {
		let nuts_objects = [];
		for (let i = 5; i > 1; i--) {
			let nuts = id.slice(0, i);
			if (nuts.length === i) {
				let nut = {
					id: nuts,
					level: i - 2,
					name: this.nuts_names[nuts] || ('NUTS ' + nuts)
				};
				nuts_objects.push(nut);
			}
		}
		return {
			nuts: nuts_objects[0],
			parents: nuts_objects.slice(1)
		};
	}
}

module.exports = Library;
