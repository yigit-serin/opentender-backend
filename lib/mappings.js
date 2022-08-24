const path = require('path');
const fs = require('fs');

const type_long = {
	type: 'long'
};

const type_float = {
	type: 'float'
};

const type_double = {
	type: 'double'
};

const type_date = {
	type: 'date'
};

const type_boolean = {
	type: 'boolean'
};

const type_string = {
    fielddata: true,
    type: 'text'
};

const type_keyword = {
    ignore_above: 256,
    type: 'keyword'
};

const def_name_slug_analyzer = {
    type: 'custom',
    tokenizer: 'whitespace',
	filter: [
		'lowercase',
		'asciifolding'
	]
};

const type_stringWithRaw = {
    fielddata: true,
    type: 'text',
    fields: {
        raw: {
            type: 'keyword'
        }
    }
};

const type_stringWithRawStopped = {
    type: 'text',
    fields: {
        raw: {
            fielddata: true,
            type: 'text'
        },
        stopped: {
            fielddata: true,
            type: 'text',
            analyzer: 'stopped_analyzer'
        }
    }
};

const type_stringWithRawAndSlug = {
    fielddata: true,
    type: 'text',
    fields: {
        raw: {
            type: 'keyword'
        },
        slug: {
            type: 'text',
            analyzer: 'slug_analyzer'
        }
    }
};


class Mapping {
	constructor(config) {
		let stopwords = fs.readFileSync(path.resolve(config.data.path, 'stopwords.txt')).toString().split('\n');
		let schema = JSON.parse(fs.readFileSync(path.resolve(config.data.shared, 'schema.json')).toString());

		const priceProperties = {
			properties: {
				netAmount: type_double,
				netAmountEur: type_double,
				netAmountNational: type_double,
				amountWithVat: type_double,
				maxAmountWithVat: type_double,
				minAmountWithVat: type_double,
				maxNetAmount: type_double,
				minNetAmount: type_double,
				vat: type_double,
				currency: type_keyword,
				currencyNational: type_keyword,
				publicationDate: type_date
			}
		};

		const cpvProperties = {
			type: 'text',
            fielddata: true,
			fields: {
				raw: {
                    fielddata: true,
                    type: 'text'
                },
				divisions: {
                    fielddata: true,
                    type: 'text',
                    analyzer: 'cpv_divisions_analyzer'
				},
				groups: {
                    fielddata: true,
                    type: 'text',
                    analyzer: 'cpv_groups_analyzer'
				},
				classes: {
                    fielddata: true,
                    type: 'text',
                    analyzer: 'cpv_classes_analyzer'
				},
				categories: {
                    fielddata: true,
                    type: 'text',
                    analyzer: 'cpv_categories_analyzer'
				}
			}
		};

		const cpvsProperties = {
			type: 'nested',
			properties: {
				code: cpvProperties
			}
		};

		const nutsProperties = {
            fielddata: true,
            type: 'text',
            fields: {
				nuts0: {
                    fielddata: true,
                    type: 'text',
					analyzer: 'nuts0_analyzer'
				},
				nuts1: {
                    fielddata: true,
                    type: 'text',
					analyzer: 'nuts1_analyzer'
				},
				nuts2: {
                    fielddata: true,
                    type: 'text',
					analyzer: 'nuts2_analyzer'
				},
				nuts3: {
                    fielddata: true,
                    type: 'text',
					analyzer: 'nuts3_analyzer'
				},
				lau1: {
                    fielddata: true,
                    type: 'text',
					analyzer: 'nuts_lau1_analyzer'
				},
				lau2: {
                    fielddata: true,
                    type: 'text',
					analyzer: 'nuts_lau2_analyzer'
				}
			}
		};

		const addressProperties = {
			properties: {
				country: type_keyword,
				postcode: type_keyword,
				state: type_keyword,
				nuts: nutsProperties,
				street: type_string,
				rawAddress: type_string,
				city: type_stringWithRaw,
				url: type_keyword,
				ot: {
					properties: {
						nutscode: nutsProperties
					}
				}
			}
		};

		const fundingsProperties = {
			properties: {
				isEuFund: type_boolean,
				source: type_stringWithRaw,
				programme: type_stringWithRaw
			}
		};

		const awardCriteriaProperties = {
			properties: {
				name: type_string,
				description: type_string,
				isPriceRelated: type_boolean,
				weight: type_long
			}
		};

		const indicatorFieldProperties = {
			properties: {}
		};

		schema.definitions['indicator-type'].enum.forEach(id => {
			indicatorFieldProperties.properties[id] = type_float;
		});

		const scoreFieldProperties = {
			properties: {}
		};

		schema.definitions['score-type'].enum.forEach(id => {
			scoreFieldProperties.properties[id] = type_float;
		});

		const correctionProperties = {
			properties: {
				sectionNumber: type_keyword,
				placeOfModifiedText: type_string,
				original: type_string,
				replacement: type_string,
				replacementDate: type_date,
				originalDate: type_date,
                originalValue: priceProperties,
                replacementValue: priceProperties,
                originalCpvs: cpvsProperties,
				replacementCpvs: cpvsProperties,
				lotNumber: type_long
			}
		};

		const indicatorProperties = {
			type: 'nested',
			properties: {
				id: type_keyword,
                type: {
                    fielddata: true,
                    type: 'text',
                    fields: {
                        keyword: {
                            type: 'keyword'
                        }
                    }
                },
                status: type_keyword,
				value: type_float
			}
		};

		const indicatorScoresProperties = {
			type: 'nested',
			properties: {
				type: type_keyword,
				status: type_keyword,
				value: type_float
			}
		};

		const bodyProperties = {
			properties: {
				id: type_keyword,
				name: type_stringWithRawAndSlug,
				email: type_keyword,
				web: type_keyword,
				contactPoint: type_keyword,
				contactName: type_keyword,
				phone: type_keyword,
				address: addressProperties,
                indicator: {
                    properties: {
                        elementaryIntegrityIndicators: {
                            properties: {
                                tender: type_float,
                                lot: type_float
                            }
                        },
                        integrityIndicatorCompositionScore: type_float,
                        transparencyIndicatorCompositionScore: type_float,
                        elementaryTransparencyIndicators: {
                            properties: {
                                tender: type_float,
                                lot: type_float
                            }
                        }
                    }
                },
                bodyIds: {
					properties: {
						id: type_keyword,
						type: type_keyword
					}
				},
				isSme: type_boolean,
				//buyers body only
				buyerType: type_keyword,
				mainActivities: type_keyword,
                sector: {
                    properties: {
                        cpvCodes: {
                            type: 'text',
                            fielddata: true
                        },
                        cpvs: {
                            properties: {
                                key: {
                                    type: 'text',
                                    fielddata: true
                                },
                                label: {
                                    type: 'text',
                                    fielddata: true
                                }
                            }
                        },
                        mostFrequentMarket: {
                            properties: {
                                key: {
                                    type: 'text',
                                    fielddata: true
                                },
                                label: {
                                    type: 'text',
                                    fielddata: true
                                }
                            }
                        }
                    }
                }
			}
		};

		const documentsProperties = {
			properties: {
				url: type_keyword,
				language: type_keyword,
				title: type_string,
				type: type_keyword,
				publicationDateTime: type_date
			}
		};

		const publicationsProperties = {
			properties: {
				publicationDate: type_date,
				dispatchDate: type_date,
				lastUpdate: type_date,
				buyerAssignedId: type_keyword,
				language: type_keyword,
				humanReadableUrl: type_keyword,
				machineReadableUrl: type_keyword,
				sourceFormType: type_keyword,
				source: type_keyword,
				sourceId: type_keyword,
				formType: type_keyword,
				sourceTenderId: type_keyword,
				isParentTender: type_boolean,
				isValid: type_boolean,
				isIncluded: type_boolean,
				version: type_long
			}
		};

		const bidProperties = {
			type: 'nested',
			properties: {
				id: type_keyword,
				bidders: {
					type: 'nested',
					properties: bodyProperties.properties
				},
				subcontractors: {
					type: 'nested',
					properties: bodyProperties.properties
				},
				subcontractedValue: priceProperties,
				price: priceProperties,
				subcontractedProportion: type_long,
                digiwhistPrice: priceProperties,
				isSubcontracted: type_boolean,
				isConsortium: type_boolean,
				isWinning: type_boolean,
				unitPrices: {
					properties: {
						unitNumber: type_long
					}
				},
				payments: {
					properties: {
						price: priceProperties,
						paymentDate: type_date
					}
				},
                robustPrice: priceProperties
			}
		};

		const lotProperties = {
			type: 'nested',
			properties: {
				id: type_keyword,
				cpvs: cpvsProperties,
				estimatedPrice: priceProperties,
				addressOfImplementation: addressProperties,
				contractNumber: type_keyword,
				status: type_keyword,
				selectionMethod: type_keyword,
				lotId: type_keyword,
				eligibilityCriteria: type_string,
				title: type_stringWithRaw,
				description: type_string,
				cancellationReason: type_string,
				fundings: fundingsProperties,
				awardDecisionDate: type_date,
				committeeApprovalDate: type_date,
				headOfEntityApprovalDate: type_date,
				endorsementDate: type_date,
				cabinetApprovalDate: type_date,
				cancellationDate: type_date,
				completionDate: type_date,
				estimatedCompletionDate: type_date,
				contractSignatureDate: type_date,
				awardCriteria: awardCriteriaProperties,
				bidsCount: type_long,
				validBidsCount: type_long,
				estimatedDurationInMonths: type_long,
				foreignCompaniesBidsCount: type_long,
				estimatedDurationInDays: type_long,
				maxFrameworkAgreementParticipants: type_long,
				electronicBidsCount: type_long,
				bids: bidProperties
			}
		};

		const tenderProperties = {
			properties: {
				country: type_keyword,
				id: type_keyword,
				created: type_date,
				modified: type_date,
				bidDeadline: type_date,
				documentsDeadline: type_date,
				estimatedStartDate: type_date,
				estimatedCompletionDate: type_date,
				awardDecisionDate: type_date,
				awardDeadline: type_date,
				cancellationDate: type_date,
				contractSignatureDate: type_date,
				title: type_stringWithRawStopped,
				titleEnglish: type_stringWithRaw,
				description: type_string,
				deposits: type_string,
				personalRequirements: type_string,
				economicRequirements: type_string,
				technicalRequirements: type_string,
				eligibilityCriteria: type_string,
				cancellationReason: type_string,
				excessiveFrameworkAgreementJustification: type_string,
				acceleratedProcedureJustification: type_string,
				appealBodyName: type_string,
				mediationBodyName: type_string,
				eligibleBidLanguages: type_keyword,
				size: type_keyword,
                digiwhistPrice: priceProperties,
				npwpReasons: type_keyword,
				selectionMethod: type_keyword,
				awardCriteria: awardCriteriaProperties,
				fundings: fundingsProperties,
				supplyType: type_keyword,
				procedureType: type_keyword,
				nationalProcedureType: type_keyword,
				estimatedPrice: priceProperties,
				documentsPrice: priceProperties,
				finalPrice: priceProperties,
				buyers: {
					type: 'nested',
					properties: bodyProperties.properties
				},
				lots: lotProperties,
				cpvs: cpvsProperties,
				documents: documentsProperties,
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
				publications: publicationsProperties,
				addressOfImplementation: addressProperties,
				documentsLocation: addressProperties,
				awardDeadlineDuration: type_long,
				estimatedDurationInYears: type_long,
				estimatedDurationInDays: type_long,
				envisagedMaxCandidatesCount: type_long,
				envisagedCandidatesCount: type_long,
				envisagedMinCandidatesCount: type_long,
				maxFrameworkAgreementParticipants: type_long,
				estimatedDurationInMonths: type_long,
				maxBidsCount: type_long,
				isAcceleratedProcedure: type_boolean,
				isWholeTenderCancelled: type_boolean,
				isCentralProcurement: type_boolean,
				isDocumentsAccessRestricted: type_boolean,
				isEInvoiceAccepted: type_boolean,
				isJointProcurement: type_boolean,
				documentsPayable: type_boolean,
				isOnBehalfOf: type_boolean,
				hasOptions: type_boolean,
				areVariantsAccepted: type_boolean,
				isCoveredByGpa: type_boolean,
				hasLots: type_boolean,
				isDps: type_boolean,
				isFrameworkAgreement: type_boolean,
				isElectronicAuction: type_boolean,
				buyerAssignedId: type_keyword,
				corrections: correctionProperties,
				indicators: indicatorProperties,
				ot: {
					properties: {
						country: type_keyword,
						date: type_date,
						indicator: indicatorFieldProperties,
						score: scoreFieldProperties,
						cpv: cpvProperties,
						scores: indicatorScoresProperties,
                        indicators: {
                            type: 'nested',
                            properties: {
                                value: type_float
                            }
                        }

                    }
				}
			}
		};

		let analysis = {
			'analyzer': {
				'stopped_analyzer': {
					'type': 'custom',
					'tokenizer': 'standard',
					'filter': ['lowercase', 'custom_length', 'custom_stop']
				},
				'slug_analyzer': def_name_slug_analyzer,
				'cpv_divisions_analyzer': {
					'tokenizer': 'standard',
                    'filter': ['truncate_cpv_divisions']
				},
				'cpv_groups_analyzer': {
					'tokenizer': 'standard',
                    'filter': ['truncate_cpv_groups']
				},
				'cpv_classes_analyzer': {
					'tokenizer': 'standard',
                    'filter': ['truncate_cpv_classes']
				},
				'cpv_categories_analyzer': {
					'tokenizer': 'standard',
                    'filter': ['truncate_cpv_categories']
				},
				'nuts0_analyzer': {
					'tokenizer': 'standard',
                    'filter': ['truncate_nuts0']
				},
				'nuts1_analyzer': {
					'tokenizer': 'standard',
                    'filter': ['truncate_nuts1']
				},
				'nuts2_analyzer': {
					'tokenizer': 'standard',
                    'filter': ['truncate_nuts2']
				},
				'nuts3_analyzer': {
					'tokenizer': 'standard',
                    'filter': ['truncate_nuts3']
				},
				'nuts_lau1_analyzer': {
					'tokenizer': 'standard',
                    'filter': ['truncate_nuts_lau1']
				},
				'nuts_lau2_analyzer': {
					'tokenizer': 'standard',
                    'filter': ['truncate_nuts_lau2']
				}
			},
			'filter': {
				'custom_stop': {
					'type': 'stop',
					'stopwords': stopwords
				},
				'custom_length': {
					'type': 'length',
					'min': 3,
					'max': 60
				},
				'truncate_nuts0': {
					'type': 'truncate',
					'length': 2
				},
				'truncate_nuts1': {
					'type': 'truncate',
					'length': 3
				},
				'truncate_nuts2': {
					'type': 'truncate',
					'length': 4
				},
				'truncate_nuts3': {
					'type': 'truncate',
					'length': 5
				},
				'truncate_nuts_lau1': {
					'type': 'truncate',
					'length': 6
				},
				'truncate_nuts_lau2': {
					'type': 'truncate',
					'length': 7
				},
				'truncate_cpv_divisions': {
					'type': 'truncate',
					'length': 2
				},
				'truncate_cpv_groups': {
					'type': 'truncate',
					'length': 3
				},
				'truncate_cpv_classes': {
					'type': 'truncate',
					'length': 4
				},
				'truncate_cpv_categories': {
					'type': 'truncate',
					'length': 5
				}
			}
		};

		this.TENDER = {
			id: 'tender',
			id_field: 'id',
			settings: {
                index: {
                    analysis,
                    number_of_shards: 5,
                    number_of_replicas: 0,
                    refresh_interval: '1m',
                    translog: {
                        sync_interval: '120s',
                        durability: 'async'
                    }
                }

            },
            mapping: tenderProperties
		};

		this.BUYER = {
			id: 'buyer',
			id_field: 'id',
			settings: {
                index: {
                    analysis,
                    number_of_shards: 5,
                    number_of_replicas: 0,
                    refresh_interval: '1m',
                    translog: {
                        sync_interval: '120s',
                        durability: 'async'
                    }
                }
            },
			mapping: {
                properties: {
                    id: type_keyword,
                    body: bodyProperties,
                    count: type_long,
                    countries: type_keyword,
                    ot: {
                        type: 'nested',
                        properties: {
                            indicator: indicatorFieldProperties,
                            indicators: indicatorScoresProperties
                        }
                    }
                }
            }
		};

		this.SUPPLIER = {
			id: 'supplier',
			id_field: 'id',
			settings: {
                index: {
                    analysis,
                    number_of_shards: 5,
                    number_of_replicas: 0,
                    refresh_interval: '1m',
                    translog: {
                        sync_interval: '120s',
                        durability: 'async'
                    }
                }
            },
			mapping: {
                properties: {
                    id: type_keyword,
                    body: bodyProperties,
                    count: type_long,
                    countries: type_keyword,
                    ot: {
                        type: 'nested',
                        properties: {
                            indicator: indicatorFieldProperties,
                            indicators: indicatorScoresProperties
                        }
                    }
                }
            }
		};
	}
}

module.exports = Mapping;
