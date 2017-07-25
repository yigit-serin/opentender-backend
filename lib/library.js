const fs = require('fs');

const Library = function (config) {

	let cpvs_full = {};
	let cpvs_divisions = {};
	let cpvs_groups = {};
	let cpvs_categories = {};

	let cpv_list = JSON.parse(fs.readFileSync(config.data.path + '/cpvs.json').toString());
	let nuts_names = JSON.parse(fs.readFileSync(config.data.shared + '/nuts/nuts_names.json').toString());

	cpv_list.forEach(cpv => {
		let s = cpv.CODE.slice(0, 8);
		cpvs_full[s] = cpv;
		if (cpv.CODE.slice(2, 8) === '000000') {
			s = cpv.CODE.slice(0, 2);
			cpvs_divisions[s] = cpv;
		}
		if (cpv.CODE.slice(3, 8) === '00000') {
			s = cpv.CODE.slice(0, 3);
			cpvs_groups[s] = cpv;
		}
		if (cpv.CODE.slice(5, 8) === '000') {
			s = cpv.CODE.slice(0, 5);
			cpvs_categories[s] = cpv;
		}
	});

	this.getCPVName = (id, lang) => {
		let cpv = cpvs_divisions[id] || cpvs_groups[id] || cpvs_categories[id] || cpvs_full[id];
		if (cpv) {
			return this.getCPVObjectName(cpv, lang);
		}
		return 'CPV#' + id;
	};

	this.getCPVObjectName = (cpv, lang) => {
		let result = cpv[lang];
		if (!result) result = cpv['EN'];
		return result;
	};

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

	this.parseCPVs = (id, lang) => {
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
	};

	this.parseNUTS = (id) => {
		let nuts_objects = [];
		for (let i = 5; i > 1; i--) {
			let nuts = id.slice(0, i);
			if (nuts.length === i) {
				let nut = {
					id: nuts,
					level: i - 2,
					name: nuts_names[nuts] || ('NUTS ' + nuts)
				};
				nuts_objects.push(nut);
			}
		}
		return {
			nuts: nuts_objects[0],
			parents: nuts_objects.slice(1)
		};
	};

};


module.exports = Library;