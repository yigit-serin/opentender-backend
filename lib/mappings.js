const path = require('path');
const fs = require('fs');

const type_stringWithRaw = {
	type: 'string',
	fields: {
		raw: {type: 'string', index: 'not_analyzed'}
	}
};
const type_stringWithRawStopped = {
	type: 'string',
	fields: {
		raw: {type: 'string', index: 'not_analyzed'},
		stopped: {
			type: 'string',
			'analyzer': 'stopped_analyzer'
		}
	}
};

const type_stringWithRawAndSlug = {
	type: 'string',
	fields: {
		raw: {type: 'string', index: 'not_analyzed'},
		slug: {
			type: 'string',
			'analyzer': 'slug_analyzer'
		}
	}
};

const type_long = {
	type: 'long'
};
const type_date = {
	type: 'date'
};
const type_boolean = {
	type: 'boolean'
};

const type_string = {
	type: 'string'
};

const type_keyword = {
	type: 'string',
	index: 'not_analyzed'
};

const def_name_slug_analyzer = {
	'type': 'custom',
	'tokenizer': 'whitespace',
	'filter': [
		'lowercase',
		'asciifolding'
	]
};

const priceProperties = {
	properties: {
		netAmount: {type: 'double'},
		amountWithVat: {type: 'double'},
		maxAmountWithVat: {type: 'double'},
		minAmountWithVat: {type: 'double'},
		maxNetAmount: {type: 'double'},
		minNetAmount: {type: 'double'},
		currency: type_keyword
	}
};

const cpvProperties = {
	type: 'nested',
	properties: {
		code: {
			type: 'string',
			fields: {
				main: {
					type: 'string',
					analyzer: 'shorten_analyzer'
				}
			}
		}
	}
};

const addressProperties = {
	properties: {
		country: type_keyword,
		postcode: type_keyword,
		nuts: type_keyword,
		street: type_string,
		city: type_stringWithRaw
	}
};

const fundingsProperties = {
	properties: {
		programme: type_keyword
	}
};

const awardCriteriaProperties = {
	properties: {
		name: type_string,
		isPriceRelated: type_boolean,
		weight: type_long
	}
};

const indicatorProperties = {
	properties: {
		relatedEntityId: type_keyword,
		id: type_keyword,
		type: type_keyword,
		metaData: {
			properties: {
				lotTitles: type_string
			}
		}
	}
};

const bodyProperties = {
	properties: {
		id: type_keyword,
		name: type_stringWithRawAndSlug,
		groupId: type_keyword,
		email: type_keyword,
		web: type_keyword,
		contactPoint: type_keyword,
		contactName: type_keyword,
		phone: type_keyword,
		address: addressProperties,
		bodyIds: {
			properties: {
				id: type_keyword,
				type: type_keyword
			}
		},
		//buyers body only
		buyerType: type_keyword,
		mainActivities: type_keyword
	}
};


const Mapping = function (config) {
	let stopwords = fs.readFileSync(path.resolve(config.data.path, 'stopwords.txt')).toString().split('\n');
	return {
		TENDER: {
			id: 'tender',
			id_field: 'id',
			settings: {
				'index': {
					'analysis': {
						'analyzer': {
							"stopped_analyzer": {
								"type": "custom",
								"tokenizer": "standard",
								"filter": ["lowercase", "custom_length", "custom_stop"]
							},
							'slug_analyzer': def_name_slug_analyzer,
							'shorten_analyzer': {
								'tokenizer': 'standard',
								'filter': ['standard', 'truncate']
							}
						},
						'filter': {
							"custom_stop": {
								"type": "stop",
								"stopwords": stopwords
							},
							"custom_length": {
								"type": "length",
								"min": 3,
								"max": 60
							},
							'truncate': {
								'type': 'truncate',
								'length': 2
							}
						}
					}
				}
			},
			mapping: {
				tender: {
					properties: {
						country: type_keyword,
						id: type_keyword,
						created: type_date,
						modified: type_date,
						bidDeadline: type_date,
						documentsDeadline: type_date,
						estimatedStartDate: type_date,
						estimatedCompletionDate: type_date,
						title: type_stringWithRawStopped,
						titleEnglish: type_stringWithRaw,
						description: type_string,
						deposits: type_string,
						personalRequirements: type_string,
						economicRequirements: type_string,
						technicalRequirements: type_string,
						appealBodyName: type_string,
						mediationBodyName: type_string,
						eligibleBidLanguages: type_keyword,
						npwpReasons: type_keyword,
						selectionMethod: type_keyword,
						awardCriteria: awardCriteriaProperties,
						fundings: fundingsProperties,
						supplyType: type_keyword,
						procedureType: type_keyword,
						estimatedPrice: priceProperties,
						documentsPrice: priceProperties,
						finalPrice: priceProperties,
						buyers: {
							type: 'nested',
							properties: bodyProperties.properties
						},
						lots: {
							type: 'nested',
							properties: {
								id: type_keyword,
								cpvs: cpvProperties,
								estimatedPrice: priceProperties,
								addressOfImplementation: addressProperties,
								contractNumber: type_keyword,
								lotId: type_keyword,
								title: type_stringWithRaw,
								description: type_string,
								fundings: fundingsProperties,
								awardDecisionDate: type_date,
								estimatedCompletionDate: type_date,
								awardCriteria: awardCriteriaProperties,
								bidsCount: type_long,
								electronicBidsCount: type_long,
								bids: {
									type: 'nested',
									properties: {
										id: type_keyword,
										bidders: {
											type: 'nested',
											properties: bodyProperties.properties
										},
										subcontractedValue: priceProperties,
										price: priceProperties,
										subcontractedProportion: type_long,
										isSubcontracted: type_boolean,
										isWinning: type_boolean,
										unitPrices: {
											properties: {
												unitNumber: type_long
											}
										}
									}
								}
							}
						},
						cpvs: cpvProperties,
						documents: {
							properties: {
								url: type_keyword
							}
						},
						onBehalfOf: bodyProperties,
						bidsRecipient: {
							// type: 'nested',
							properties: bodyProperties.properties
						},
						furtherInformationProvider: {
							// type: 'nested',
							properties: bodyProperties.properties
						},
						specificationsProvider: {
							// type: 'nested',
							properties: bodyProperties.properties
						},
						administrators: {
							// type: 'nested',
							properties: bodyProperties.properties
						},
						publications: {
							properties: {
								publicationDate: type_date,
								dispatchDate: type_date,
								buyerAssignedId: type_keyword,
								language: type_keyword,
								humanReadableUrl: type_keyword,
								sourceFormType: type_keyword,
								source: type_keyword,
								sourceId: type_keyword,
								formType: type_keyword,
								sourceTenderId: type_keyword,
								isIncluded: type_boolean
							}
						},
						estimatedDurationInDays: type_long,
						maxFrameworkAgreementParticipants: type_long,
						estimatedDurationInMonths: type_long,
						maxBidsCount: type_long,
						isAcceleratedProcedure: type_boolean,
						documentsPayable: type_boolean,
						isOnBehalfOf: type_boolean,
						hasOptions: type_boolean,
						areVariantsAccepted: type_boolean,
						isCoveredByGpa: type_boolean,
						hasLots: type_boolean,
						isDps: type_boolean,
						isFrameworkAgreement: type_boolean,
						isElectronicAuction: type_boolean,
						indicators: indicatorProperties
					}
				}
			}
		},
		PUBLICBODY: {
			id: 'publicbody',
			id_field: 'id',
			settings: {},
			mapping: {
				publicbody: {
					properties: {
						id: type_keyword,
						name: type_stringWithRaw,
						country: type_keyword
					}
				}
			}
		},
		COMPANY: {
			id: 'company',
			id_field: 'body.groupId',
			settings: {
				'index': {
					'analysis': {
						'analyzer': {
							'slug_analyzer': def_name_slug_analyzer
						}
					}
				}
			},
			mapping: {
				company: {
					properties: {
						country: type_keyword,
						body: bodyProperties,
						source: {
							properties: {
								tender: type_keyword
							}
						}
					}
				}
			}
		},
		AUTHORITY: {
			id: 'authority',
			id_field: 'body.groupId',
			settings: {
				'index': {
					'analysis': {
						'analyzer': {
							'slug_analyzer': def_name_slug_analyzer
						}
					}
				}
			},
			mapping: {
				authority: {
					properties: {
						country: type_keyword,
						body: bodyProperties,
						source: {
							properties: {
								tender: type_keyword
							}
						}
					}
				}
			}
		}
	};
};


module.exports = Mapping;