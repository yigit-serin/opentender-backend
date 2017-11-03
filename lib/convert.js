// http://ec.europa.eu/eurostat/documents/345175/629341/1999-2003.xls
// http://ec.europa.eu/eurostat/documents/345175/629341/2006-2010.xls
// http://ec.europa.eu/eurostat/documents/345175/629341/NUTS+2010+-+NUTS+2013.xls

let nuts_greece = [
	// https://en.wikipedia.org/wiki/NUTS_statistical_regions_of_Greece
	// https://de.wikipedia.org/wiki/NUTS:EL
	{src: 'GR', dest: 'EL', name: 'ΕΛΛΑΔΑ (ELLADA)'},
	{src: 'GR1', dest: 'EL5', name: 'ΒΟΡΕΙΑ ΕΛΛΑΔΑ (VOREIA ELLADA)'},
	{src: 'GR11', dest: 'EL51', name: 'Aνατολική Μακεδονία, Θράκη (Anatoliki Makedonia, Thraki)'},
	{src: 'GR111', dest: 'EL511', name: 'Έβρος (Evros)'},
	{src: 'GR112', dest: 'EL512', name: 'Ξάνθη (Xanthi)'},
	{src: 'GR113', dest: 'EL513', name: 'Ροδόπη (Rodopi)'},
	{src: 'GR114', dest: 'EL514', name: 'Δράμα (Drama)'},
	{src: 'GR115', dest: 'EL515', name: 'Θάσος, Καβάλα (Thasos, Kavala)'},
	{src: 'GR12', dest: 'EL52', name: 'Κεντρική Μακεδονία (Kentriki Makedonia)'},
	{src: 'GR121', dest: 'EL521', name: 'Ημαθία (Imathia)'},
	{src: 'GR122', dest: 'EL522', name: 'Θεσσαλονίκη (Thessaloniki)'},
	{src: 'GR123', dest: 'EL523', name: 'Κιλκίς (Kilkis)'},
	{src: 'GR124', dest: 'EL524', name: 'Πέλλα (Pella)'},
	{src: 'GR125', dest: 'EL525', name: 'Πιερία (Pieria)'},
	{src: 'GR126', dest: 'EL526', name: 'Σέρρες (Serres)'},
	{src: 'GR127', dest: 'EL527', name: 'Χαλκιδική (Chalkidiki)'},
	{src: 'GR13', dest: 'EL53', name: 'Δυτική Μακεδονία (Dytiki Makedonia)'},
	{src: 'GR131', dest: 'EL531', name: 'Γρεβενά, Κοζάνη (Grevena)'},
	{src: 'GR132', dest: 'EL532', name: 'Καστοριά (Kastoria)'},
	{src: 'GR133', dest: 'EL531', name: 'Γρεβενά, Κοζάνη (Kozani)'},
	{src: 'GR134', dest: 'EL533', name: 'Φλώρινα (Florina)'},
	{src: 'GR14', dest: 'EL61', name: 'Θεσσαλία (Thessalia)'},
	{src: 'GR141', dest: 'EL611', name: 'Καρδίτσα, Τρίκαλα (Karditsa)'},
	{src: 'GR142', dest: 'EL612', name: 'Λάρισα (Larisa)'},
	{src: 'GR143', dest: 'EL613', name: 'Μαγνησία, Σποράδες (Magnisia, Sporades)'},
	{src: 'GR144', dest: 'EL611', name: 'Καρδίτσα, Τρίκαλα (Trikala)'},
	{src: 'GR21', dest: 'EL54', name: 'Ήπειρος (Ipeiros)'},
	{src: 'GR211', dest: 'EL541', name: 'Άρτα, Πρέβεζα (Arta)'},
	{src: 'GR212', dest: 'EL542', name: 'Θεσπρωτία (Thesprotia)'},
	{src: 'GR213', dest: 'EL543', name: 'Ιωάννινα (Ioannina)'},
	{src: 'GR214', dest: 'EL541', name: 'Άρτα, Πρέβεζα (Preveza)'},
	{src: 'GR22', dest: 'EL62', name: 'Ιόνια Νησιά (Ionia Nisia)'},
	{src: 'GR221', dest: 'EL621', name: 'Ζάκυνθος (Zakynthos)'},
	{src: 'GR222', dest: 'EL622', name: 'Κέρκυρα (Kerkyra)'},
	{src: 'GR223', dest: 'EL623', name: 'Ιθάκη, Κεφαλληνία (Ithaki, Kefallinia)'},
	{src: 'GR224', dest: 'EL624', name: 'Λευκάδα (Lefkada)'},
	{src: 'GR23', dest: 'EL63', name: 'Δυτική Ελλάδα (Dytiki Ellada)'},
	{src: 'GR231', dest: 'EL631', name: 'Αιτωλοακαρνανία (Aitoloakarnania)'},
	{src: 'GR232', dest: 'EL632', name: 'Αχαΐα (Achaia)'},
	{src: 'GR233', dest: 'EL633', name: 'Ηλεία (Ileia)'},
	{src: 'GR24', dest: 'EL64', name: 'Στερεά Ελλάδα (Sterea Ellada)'},
	{src: 'GR241', dest: 'EL641', name: 'Βοιωτία (Voiotia)'},
	{src: 'GR242', dest: 'EL642', name: 'Εύβοια (Evvoia)'},
	{src: 'GR243', dest: 'EL643', name: 'Ευρυτανία (Evrytania)'},
	{src: 'GR244', dest: 'EL644', name: 'Φθιώτιδα (Fthiotida)'},
	{src: 'GR245', dest: 'EL645', name: 'Φωκίδα (Fokida)'},
	{src: 'GR25', dest: 'EL65', name: 'Πελοπόννησος (Peloponnisos)'},
	{src: 'GR251', dest: 'EL651', name: 'Αργολίδα, Αρκαδία (Argolida)'},
	{src: 'GR252', dest: 'EL651', name: 'Αργολίδα, Αρκαδία (Arkadia)'},
	{src: 'GR253', dest: 'EL652', name: 'Κορινθία (Korinthia)'},
	{src: 'GR254', dest: 'EL653', name: 'Λακωνία, Μεσσηνία (Lakonia)'},
	{src: 'GR255', dest: 'EL653', name: 'Λακωνία, Μεσσηνία (Messinia)'},
	{src: 'GR3', dest: 'EL3', name: 'ATTIKΗ (ATTIKI)'},
	{src: 'GR30', dest: 'EL30', name: 'Aττική (Attiki)'},
	{src: 'GR300', dest: 'EL30', name: 'Aττική (Attiki)'},
	{src: 'GR301', dest: 'EL301', name: 'Βόρειος Τομέας Αθηνών (Voreios Tomeas Athinon)'},
	{src: 'GR302', dest: 'EL302', name: 'Δυτικός Τομέας Αθηνών (Dytikos Tomeas Athinon)'},
	{src: 'GR303', dest: 'EL303', name: 'Κεντρικός Τομέας Αθηνών (Kentrikos Tomeas Athinon)'},
	{src: 'GR304', dest: 'EL304', name: 'Νότιος Τομέας Αθηνών (Notios Tomeas Athinon)'},
	{src: 'GR305', dest: 'EL305', name: 'Ανατολική Αττική (Anatoliki Attiki)'},
	{src: 'GR306', dest: 'EL306', name: 'Δυτική Αττική (Dytiki Attiki)'},
	{src: 'GR307', dest: 'EL307', name: 'Πειραιάς, Νήσοι (Peiraias, Nisoi)'},
	{src: 'GR4', dest: 'EL4', name: 'NΗΣΙΑ ΑΙΓΑΙΟΥ, KΡΗΤΗ (NISIA AIGAIOU, KRITI)'},
	{src: 'GR41', dest: 'EL41', name: 'Βόρειο Αιγαίο (Voreio Aigaio)'},
	{src: 'GR411', dest: 'EL411', name: 'Λέσβος, Λήμνος (Lesvos, Limnos)'},
	{src: 'GR412', dest: 'EL412', name: 'Ικαρία, Σάμος (Ikaria, Samos)'},
	{src: 'GR413', dest: 'EL413', name: 'Χίος (Chios)'},
	{src: 'GR42', dest: 'EL42', name: 'Νότιο Αιγαίο (Notio Aigaio)'},
	{src: 'GR421', dest: 'EL421', name: 'Κάλυμνος, Κάρπαθος, Κως, Ρόδος (Kalymnos, Karpathos, Kos, Rodos)'},
	{src: 'GR422', dest: 'EL422', name: 'Άνδρος, Θήρα, Κέα, Μήλος, Μύκονος, Νάξος, Πάρος, Σύρος, Τήνος (Andros, Thira, Kea, Milos, Mykonos, Naxos, Paros,  Syros, Tinos)'},
	{src: 'GR43', dest: 'EL43', name: 'Κρήτη (Kriti)'},
	{src: 'GR431', dest: 'EL431', name: 'Ηράκλειο (Irakleio)'},
	{src: 'GR432', dest: 'EL432', name: 'Λασίθι (Lasithi)'},
	{src: 'GR433', dest: 'EL433', name: 'Ρεθύμνη (Rethymni)'},
	{src: 'GR434', dest: 'EL434', name: 'Χανιά (Chania)'},
	{src: 'GR5', dest: 'EL5', name: 'ΒΟΡΕΙΑ ΕΛΛΑΔΑ (VOREIA ELLADA)'},
	{src: 'GR51', dest: 'EL51', name: 'Aνατολική Μακεδονία, Θράκη (Anatoliki Makedonia, Thraki)'},
	{src: 'GR511', dest: 'EL511', name: 'Έβρος (Evros)'},
	{src: 'GR512', dest: 'EL512', name: 'Ξάνθη (Xanthi)'},
	{src: 'GR513', dest: 'EL513', name: 'Ροδόπη (Rodopi)'},
	{src: 'GR514', dest: 'EL514', name: 'Δράμα (Drama)'},
	{src: 'GR515', dest: 'EL515', name: 'Θάσος, Καβάλα (Thasos, Kavala)'},
	{src: 'GR52', dest: 'EL52', name: 'Κεντρική Μακεδονία (Kentriki Makedonia)'},
	{src: 'GR521', dest: 'EL521', name: 'Ημαθία (Imathia)'},
	{src: 'GR522', dest: 'EL522', name: 'Θεσσαλονίκη (Thessaloniki)'},
	{src: 'GR523', dest: 'EL523', name: 'Κιλκίς (Kilkis)'},
	{src: 'GR524', dest: 'EL524', name: 'Πέλλα (Pella)'},
	{src: 'GR525', dest: 'EL525', name: 'Πιερία (Pieria)'},
	{src: 'GR526', dest: 'EL526', name: 'Σέρρες (Serres)'},
	{src: 'GR527', dest: 'EL527', name: 'Χαλκιδική (Chalkidiki)'},
	{src: 'GR53', dest: 'EL53', name: 'Δυτική Μακεδονία (Dytiki Makedonia)'},
	{src: 'GR531', dest: 'EL531', name: 'Γρεβενά, Κοζάνη (Grevena, Kozani)'},
	{src: 'GR532', dest: 'EL532', name: 'Καστοριά (Kastoria)'},
	{src: 'GR533', dest: 'EL533', name: 'Φλώρινα (Florina)'},
	{src: 'GR54', dest: 'EL54', name: 'Ήπειρος (Ipeiros)'},
	{src: 'GR541', dest: 'EL541', name: 'Άρτα, Πρέβεζα (Arta, Preveza)'},
	{src: 'GR542', dest: 'EL542', name: 'Θεσπρωτία (Thesprotia)'},
	{src: 'GR543', dest: 'EL543', name: 'Ιωάννινα (Ioannina)'},
	{src: 'GR6', dest: 'EL6', name: 'ΚΕΝΤΡΙΚΗ ΕΛΛΑΔΑ (KENTRIKI ELLADA)'},
	{src: 'GR61', dest: 'EL61', name: 'Θεσσαλία (Thessalia)'},
	{src: 'GR611', dest: 'EL611', name: 'Καρδίτσα, Τρίκαλα (Karditsa, Trikala)'},
	{src: 'GR612', dest: 'EL612', name: 'Λάρισα (Larisa)'},
	{src: 'GR613', dest: 'EL613', name: 'Μαγνησία, Σποράδες (Magnisia, Sporades)'},
	{src: 'GR62', dest: 'EL62', name: 'Ιόνια Νησιά (Ionia Nisia)'},
	{src: 'GR621', dest: 'EL621', name: 'Ζάκυνθος (Zakynthos)'},
	{src: 'GR622', dest: 'EL622', name: 'Κέρκυρα (Kerkyra)'},
	{src: 'GR623', dest: 'EL623', name: 'Ιθάκη, Κεφαλληνία (Ithaki, Kefallinia)'},
	{src: 'GR624', dest: 'EL624', name: 'Λευκάδα (Lefkada)'},
	{src: 'GR63', dest: 'EL63', name: 'Δυτική Ελλάδα (Dytiki Ellada)'},
	{src: 'GR631', dest: 'EL631', name: 'Αιτωλοακαρνανία (Aitoloakarnania)'},
	{src: 'GR632', dest: 'EL632', name: 'Αχαΐα (Achaia)'},
	{src: 'GR633', dest: 'EL633', name: 'Ηλεία (Ileia)'},
	{src: 'GR64', dest: 'EL64', name: 'Στερεά Ελλάδα (Sterea Ellada)'},
	{src: 'GR641', dest: 'EL641', name: 'Βοιωτία (Voiotia)'},
	{src: 'GR642', dest: 'EL642', name: 'Εύβοια (Evvoia)'},
	{src: 'GR643', dest: 'EL643', name: 'Ευρυτανία (Evrytania)'},
	{src: 'GR644', dest: 'EL644', name: 'Φθιώτιδα (Fthiotida)'},
	{src: 'GR645', dest: 'EL645', name: 'Φωκίδα (Fokida)'},
	{src: 'GR65', dest: 'EL65', name: 'Πελοπόννησος (Peloponnisos)'},
	{src: 'GR651', dest: 'EL651', name: 'Αργολίδα, Αρκαδία (Argolida, Arkadia)'},
	{src: 'GR652', dest: 'EL652', name: 'Κορινθία (Korinthia)'},
	{src: 'GR653', dest: 'EL653', name: 'Λακωνία, Μεσσηνία (Lakonia, Messinia)'},
	{src: 'GRZ', dest: 'ELZ', name: 'EXTRA-REGIO NUTS 1'},
	{src: 'GRZZ', dest: 'ELZZ', name: 'Extra-Regio NUTS 2'},
	{src: 'GRZZZ', dest: 'ELZZZ', name: 'Extra-Regio NUTS 3'}
];

let nuts_portugal = [
	// https://pt.wikipedia.org/wiki/NUTS_de_Portugal
	// https://cs.wikipedia.org/wiki/PT-NUTS
	{src: 'PT113', dest: 'PT119', name: 'Ave'},
	{src: 'PT114', dest: 'PT11A', name: 'Grande Porto'},
	{src: 'PT115', dest: 'PT11B', name: 'Tâmega'}, // split into PT11B && PT11C
	{src: 'PT116', dest: 'PT11D', name: 'Entre Douro e Vouga'},
	{src: 'PT117', dest: 'PT11D', name: 'Douro'},
	{src: 'PT118', dest: 'PT11E', name: 'Alto Trás-os-Montes'},
	{src: 'PT161', dest: 'PT16D', name: 'Baixo Vouga'},
	{src: 'PT162', dest: 'PT16D', name: 'Baixo Mondego'},
	{src: 'PT163', dest: 'PT16F', name: 'Pinhal Litoral'},
	{src: 'PT164', dest: 'PT16E', name: 'Pinhal Interior Norte'},
	{src: 'PT165', dest: 'PT16G', name: 'Dão-Lafões'},
	{src: 'PT166', dest: 'PT16F', name: 'Pinhal Interior Sul'},
	{src: 'PT167', dest: 'PT16J', name: 'Serra da Estrela'},
	{src: 'PT168', dest: 'PT16J', name: 'Beira Interior Norte'},
	{src: 'PT169', dest: 'PT16J', name: 'Beira Interior Sul'},
	{src: 'PT16A', dest: 'PT16J', name: 'Cova da Beira'},
	{src: 'PT16C', dest: 'PT16I', name: 'Médio Tejo'},
	{src: 'PT171', dest: 'PT170', name: 'Grande Lisboa'},
	{src: 'PT172', dest: 'PT170', name: 'Península de Setúbal'},
	{src: 'PT182', dest: 'PT186', name: 'Alto Alentejo'},
	{src: 'PT183', dest: 'PT187', name: 'Alentejo Central'}
	// PT164
];

let nuts_netherlands = [
	// https://en.wikipedia.org/wiki/NUTS_statistical_regions_of_the_Netherlands
	{src: 'NL336', dest: 'NL33A', name: 'South South Holland'},
	{src: 'NL335', dest: 'NL339', name: 'Rijnmond'},
	{src: 'NL331', dest: 'NL337', name: 'Leiden and Bollenstreek'}
];

let nuts_croatia = [
	// http://database.espon.eu/db2/jsf/DicoSpatialUnits/DicoSpatialUnits_html/ch01s01.html
	{src: 'HR011', dest: 'HR041', name: 'Grad Zagreb'},
	{src: 'HR012', dest: 'HR042', name: 'Zagrebačka županija'},
	{src: 'HR013', dest: 'HR043', name: 'Krapinsko-zagorska županija'},
	{src: 'HR014', dest: 'HR044', name: 'Varaždinska županija'},
	{src: 'HR015', dest: 'HR045', name: 'Koprivničko-križevačka županija'},
	{src: 'HR016', dest: 'HR046', name: 'Međimurska županija'},
	{src: 'HR021', dest: 'HR047', name: 'Bjelovarsko-bilogorska županija'},
	{src: 'HR022', dest: 'HR048', name: 'Virovitičko-podravska županija'},
	{src: 'HR023', dest: 'HR049', name: 'Požeško-slavonska županija'},
	{src: 'HR024', dest: 'HR04A', name: 'Brodsko-posavska županija'},
	{src: 'HR025', dest: 'HR04B', name: 'Osječko-baranjska županija'},
	{src: 'HR026', dest: 'HR04C', name: 'Vukovarsko-srijemska županija'},
	{src: 'HR027', dest: 'HR04D', name: 'Karlovačka županija'},
	{src: 'HR028', dest: 'HR04E', name: 'Sisačko-moslavačka županija'}
];

let nuts_france = [
	// https://fr.wikipedia.org/wiki/Liste_des_r%C3%A9gions_de_l%27Union_europ%C3%A9enne,_de_l%27Association_europ%C3%A9enne_de_libre-%C3%A9change_et_des_pays_candidats_%C3%A0_l%27adh%C3%A9sion_%C3%A0_l%27Union_europ%C3%A9enne
	{src: 'FR9', dest: 'FRA', name: 'Départements d\'Outre Mer'},
	{src: 'FR91', dest: 'FRA1', name: 'Guadeloupe'},
	{src: 'FR910', dest: 'FRA10', name: 'Guadeloupe'},
	{src: 'FR92', dest: 'FRA2', name: 'Martinique'},
	{src: 'FR920', dest: 'FRA20', name: 'Martinique'},
	{src: 'FR93', dest: 'FRA3', name: 'Guyane'},
	{src: 'FR930', dest: 'FRA30', name: 'Guyane'},
	{src: 'FR94', dest: 'FRA4', name: 'La Réunion'},
	{src: 'FR940', dest: 'FRA40', name: 'La Réunion'}
];

let nuts_uk = [
	// https://en.wikipedia.org/wiki/NUTS_statistical_regions_of_the_United_Kingdom
	// http://www.eurotender.de/php/nutcodes-dephp/nutscodedeUK.php
	{src: 'UKD21', dest: 'UKD61'},
	{src: 'UKD22', dest: 'UKD63'}, //split into UKD63 && UKD62
	{src: 'UKD52', dest: 'UKD72'},
	{src: 'UKD53', dest: 'UKD73'},
	{src: 'UKD54', dest: 'UKD74'},
	{src: 'UKD51', dest: 'UKD71'},
	{src: 'UKD2', dest: 'UKD6'},
	{src: 'UKD5', dest: 'UKD7'},
	{src: 'UKD43', dest: 'UKD44'}, // split into UKD44 && UKD45 && UKD46 && UKD47
	{src: 'UKJ33', dest: 'UKJ36'}, // split into UKJ35 && UKJ36 && UKJ37
	{src: 'UKD32', dest: 'UKD36'}, // split into UKD36 && UKD37
	{src: 'UKJ24', dest: 'UKJ27'}, // split into UKJ27 && UKJ28
	{src: 'UKJ23', dest: 'UKJ25'}, // split into UKJ25 && UKJ26
	{src: 'UKE43', dest: 'UKE44'}, // split into UKE44 && UKE45
	{src: 'UKF23', dest: 'UKF24'}, // split into UKF24 && UKF25
	{src: 'UKG34', dest: 'UKG36'}, // split into UKG36 && UKG37
	{src: 'UKG35', dest: 'UKG38'}, // split into UKG38 && UKG39
	{src: 'UKH22', dest: 'UKH24'}, // split into UKH24 && UKH25
	{src: 'UKH13', dest: 'UKH15'}, // split into UKH15 && UKH16 && UKH17
	{src: 'UKH33', dest: 'UKH34'}, // split into UKH34 && UKH35 && UKH36 && UKH37
	{src: 'UKJ42', dest: 'UKJ43'}, // split into UKJ43 && UKJ44 && UKJ45 && UKJ46
	{src: 'UKD31', dest: 'UKD33'}, // split into UKD33 && UKD34 && UKD35

	{src: 'UKI12', dest: 'UKI4'},
	{src: 'UKI21', dest: 'UKI5'},
	{src: 'UKI22', dest: 'UKI6'},
	{src: 'UKI23', dest: 'UKI7'},
	{src: 'UKI11', dest: 'UKI31'},
	{src: 'UKI12', dest: 'UKI32'},
	{src: 'UKI1', dest: 'UKI'},
	{src: 'UKI2', dest: 'UKI'}
];

let nuts_finland = [
	// http://www.eurotender.de/php/nutcodes-dephp/nutscodedeFI.php
	{src: 'FI131', dest: 'FI1D1', name: 'Etelä-Savo'},
	{src: 'FI132', dest: 'FI1D2', name: 'Pohjois-Savo'},
	{src: 'FI133', dest: 'FI1D3', name: 'Pohjois-Karjala'},
	{src: 'FI134', dest: 'FI1D4', name: 'Kainuu'},
	{src: 'FI181', dest: 'FI1B', name: 'Uusimaa'},
	{src: 'FI182', dest: 'FI1B', name: 'Itä-Uusimaa'},
	{src: 'FI183', dest: 'FI1C1', name: 'Varsinais-Suomi'},
	{src: 'FI184', dest: 'FI1C2', name: 'Kanta-Häme'},
	{src: 'FI185', dest: 'FI1C3', name: 'Päijät-Häme'},
	{src: 'FI186', dest: 'FI1C4', name: 'Kymenlaakso'},
	{src: 'FI187', dest: 'FI1C5', name: 'Etelä-Karjala'},
	{src: 'FI1A1', dest: 'FI1D5', name: 'Keski-Pohjanmaa'},
	{src: 'FI1A2', dest: 'FI1D6', name: 'Pohjois-Pohjanmaa'},
	{src: 'FI1A3', dest: 'FI1D7', name: 'Lappi'},
	{src: 'FI18', dest: 'FI1C', name: 'Etelä-Suomi'},
	{src: 'FI1A', dest: 'FI1D', name: 'Pohjois-Suomi'},
	{src: 'FI13', dest: 'FI1D', name: 'Itä-Suomi'}
];

let nuts_italy = [
	// https://de.wikipedia.org/wiki/NUTS:IT
	// https://it.wikipedia.org/wiki/Nomenclatura_delle_Unità_Territoriali_per_le_Statistiche_dell'Italia
	{src: 'ITD', dest: 'ITH', name: 'Nord-Est'},
	{src: 'ITD1', dest: 'ITH1', name: 'Alto Adige'},
	{src: 'ITD10', dest: 'ITH10', name: 'Bozen/Bolzano'},
	{src: 'ITD2', dest: 'ITH2', name: 'Trento/Trient'},
	{src: 'ITD20', dest: 'ITH20', name: 'Trento'},
	{src: 'ITD3', dest: 'ITH3', name: 'Veneto'},
	{src: 'ITD31', dest: 'ITH31', name: 'Verona'},
	{src: 'ITD32', dest: 'ITH32', name: 'Vicenza'},
	{src: 'ITD33', dest: 'ITH33', name: 'Belluno'},
	{src: 'ITD34', dest: 'ITH34', name: 'Treviso'},
	{src: 'ITD35', dest: 'ITH35', name: 'Venezia'},
	{src: 'ITD36', dest: 'ITH36', name: 'Padova'},
	{src: 'ITD37', dest: 'ITH37', name: 'Rovigo'},
	{src: 'ITD4', dest: 'ITH4', name: 'Friuli-Venezia Giulia'},
	{src: 'ITD41', dest: 'ITH41', name: 'Pordenone'},
	{src: 'ITD42', dest: 'ITH42', name: 'Udine'},
	{src: 'ITD43', dest: 'ITH43', name: 'Gorizia'},
	{src: 'ITD44', dest: 'ITH44', name: 'Trieste'},
	{src: 'ITD5', dest: 'ITH5', name: 'Emilia-Romagna'},
	{src: 'ITD51', dest: 'ITH51', name: 'Piacenza'},
	{src: 'ITD52', dest: 'ITH52', name: 'Parma'},
	{src: 'ITD53', dest: 'ITH53', name: 'Reggio Emilia'},
	{src: 'ITD54', dest: 'ITH54', name: 'Modena'},
	{src: 'ITD55', dest: 'ITH55', name: 'Bologna'},
	{src: 'ITD56', dest: 'ITH56', name: 'Ferrara'},
	{src: 'ITD57', dest: 'ITH57', name: 'Ravenna'},
	{src: 'ITD58', dest: 'ITH58', name: 'Forlì-Cesena'},
	{src: 'ITD59', dest: 'ITH59', name: 'Rimini'},
	{src: 'ITE', dest: 'ITI', name: 'Centro'},
	{src: 'ITE1', dest: 'ITI1', name: 'Toscana'},
	{src: 'ITE11', dest: 'ITI11', name: 'Massa-Carrara'},
	{src: 'ITE12', dest: 'ITI12', name: 'Lucca'},
	{src: 'ITE13', dest: 'ITI13', name: 'Pistoia'},
	{src: 'ITE14', dest: 'ITI14', name: 'Firenze'},
	{src: 'ITE15', dest: 'ITI15', name: 'Prato'},
	{src: 'ITE16', dest: 'ITI16', name: 'Livorno'},
	{src: 'ITE17', dest: 'ITI17', name: 'Pisa'},
	{src: 'ITE18', dest: 'ITI18', name: 'Arezzo'},
	{src: 'ITE19', dest: 'ITI19', name: 'Siena'},
	{src: 'ITE1A', dest: 'ITI1A', name: 'Grosseto'},
	{src: 'ITE2', dest: 'ITI2', name: 'Umbria'},
	{src: 'ITE21', dest: 'ITI21', name: 'Perugia'},
	{src: 'ITE22', dest: 'ITI22', name: 'Terni'},
	{src: 'ITE3', dest: 'ITI3', name: 'Marche'},
	{src: 'ITE31', dest: 'ITI31', name: 'Pesaro-Urbino'},
	{src: 'ITE32', dest: 'ITI32', name: 'Ancona'},
	{src: 'ITE33', dest: 'ITI33', name: 'Macerata'},
	{src: 'ITE34', dest: 'ITI34', name: 'Ascoli Piceno'},
	{src: 'ITE4', dest: 'ITI4', name: 'Lazio'},
	{src: 'ITE41', dest: 'ITI41', name: 'Viterbo'},
	{src: 'ITE42', dest: 'ITI42', name: 'Rieti'},
	{src: 'ITE43', dest: 'ITI43', name: 'Roma'},
	{src: 'ITE44', dest: 'ITI44', name: 'Latina'},
	{src: 'ITE45', dest: 'ITI45', name: 'Frosinone'},

	{src: 'ITC45', dest: 'ITC4C', name: 'Milano'},
	{src: 'ITF41', dest: 'ITF46', name: 'Foggia'},
	{src: 'ITF42', dest: 'ITF47', name: 'Bari'}
];

let nuts_poland = [
	// https://en.wikipedia.org/wiki/NUTS_statistical_regions_of_Poland
	{src: 'PL121', dest: 'PL12B', name: 'Ciechanowsko-płocki'}, // split into PL12B && PL12C
	{src: 'PL122', dest: 'PL12D', name: 'Ostrołęcko-siedlecki'}, // split into PL12D 6& PL12E
	{src: 'PL215', dest: 'PL218', name: 'Nowosądecki'},
	{src: 'PL216', dest: 'PL21A', name: 'Oświęcimski'},
	{src: 'PL422', dest: 'PL426', name: 'Koszaliński'},
	{src: 'PL423', dest: 'PL638', name: 'Stargardzki'},
	{src: 'PL425', dest: 'PL428', name: 'Szczeciński'},
	{src: 'PL521', dest: 'PL523', name: 'Nyski'},
	{src: 'PL522', dest: 'PL524', name: 'Opolski'},
	{src: 'PL614', dest: 'PL616', name: 'Grudziądzki'},
	{src: 'PL615', dest: 'PL619', name: 'Włocławski'},
	{src: 'PL631', dest: 'PL636', name: 'Słupski'},
	{src: 'PL635', dest: 'PL638', name: 'Starogardzki'}
];

let nuts_germany = [
	// http://www.eurotender.de/php/nutcodes-dephp/nutscodedeDE.php
	// https://blog.cosinex.de/2017/07/27/neue-nuts-codes-fuer-eu-weite-ausschreibungen/
	{src: 'DE411', dest: 'DE403', name: 'FRANKFURT (ODER)'},
	{src: 'DE412', dest: 'DE405', name: 'BARNIM'},
	{src: 'DE413', dest: 'DE409', name: 'MAERKISCH-ODERLAND'},
	{src: 'DE414', dest: 'DE40A', name: 'OBERHAVEL'},
	{src: 'DE415', dest: 'DE40C', name: 'ODER-SPREE'},
	{src: 'DE416', dest: 'DE40D', name: 'OSTPRIGNITZ-RUPPIN'},
	{src: 'DE417', dest: 'DE40F', name: 'PRIGNITZ'},
	{src: 'DE418', dest: 'DE40I', name: 'UCKERMARK'},
	{src: 'DE41', dest: 'DE40', name: 'BRANDENBURG NORDOST'},

	{src: 'DE421', dest: 'DE401', name: 'BRANDENBURG AN DER HAVEL, KREISFREIE STADT'},
	{src: 'DE422', dest: 'DE402', name: 'COTTBUS, KREISFREIE STADT'},
	{src: 'DE423', dest: 'DE404', name: 'POTSDAMM, KREISFREIE STADT'},
	{src: 'DE424', dest: 'DE406', name: 'DAHME-SPREEWALD'},
	{src: 'DE425', dest: 'DE407', name: 'ELBE-ELSTER'},
	{src: 'DE426', dest: 'DE408', name: 'HAVELLAND'},
	{src: 'DE427', dest: 'DE40B', name: 'OBERSPREEWALD-LAUSITZ'},
	{src: 'DE428', dest: 'DE40E', name: 'POTSDAM-MITTELMARK'},
	{src: 'DE429', dest: 'DE40G', name: 'SPREE-NEISSE'},
	{src: 'DE42A', dest: 'DE40H', name: 'TELTOW-FLAEMING'},

	{src: 'DE801', dest: 'DE80N', name: 'GREIFSWALD, KRFR.ST.'},
	{src: 'DE802', dest: 'DE80J', name: 'NEUBRANDENBURG, KRFR.ST.'},
	{src: 'DE805', dest: 'DE80L', name: 'STRALSUND, KRFR.ST.'},
	{src: 'DE806', dest: 'DE80M', name: 'WISMAR, KRFR.ST.'},
	{src: 'DE807', dest: 'DE80K', name: 'BAD DOBERAN'},
	{src: 'DE808', dest: 'DE80J', name: 'DEMMIN'},
	{src: 'DE809', dest: 'DE80K', name: 'GUESTROW'},
	{src: 'DE80A', dest: 'DE80O', name: 'LUDWIGSLUST'},
	{src: 'DE80B', dest: 'DE80J', name: 'MECKLENBURG-STRELITZ'},
	{src: 'DE80C', dest: 'DE80J', name: 'MUERITZ'},
	{src: 'DE80D', dest: 'DE80L', name: 'NORDVORPOMMERN'},
	{src: 'DE80E', dest: 'DE80M', name: 'NORDWESTMECKLENBURG'},
	{src: 'DE80F', dest: 'DE80N', name: 'OSTVORPOMMERN'},
	{src: 'DE80G', dest: 'DE80O', name: 'PARCHIM'},
	{src: 'DE80H', dest: 'DE80L', name: 'RUEGEN'},
	{src: 'DE80I', dest: 'DE80N', name: 'UECKER-RANDOW'},

	{src: 'DEA21', dest: 'DEA2D', name: 'Aachen, Kreisfreie Stadt'},
	{src: 'DEA25', dest: 'DEA2D', name: 'Aachen, Kreis'},

	{src: 'DED11', dest: 'DED41', name: 'Chemnitz, Kreisfreie Stadt'},
	{src: 'DED12', dest: 'DED44', name: 'Plauen, Kreisfreie Stadt'},
	{src: 'DED13', dest: 'DED45', name: 'Zwickau, Kreisfreie Stadt'},
	{src: 'DED14', dest: 'DED42', name: 'Annaberg'},
	{src: 'DED15', dest: 'DED45', name: 'Chemnitzer Land'},
	{src: 'DED16', dest: 'DED43', name: 'Freiberg'},
	{src: 'DED17', dest: 'DED44', name: 'Vogtlandkreis'},
	{src: 'DED18', dest: 'DED42', name: 'Mittlerer Erzgebirgskreis'},
	{src: 'DED19', dest: 'DED43', name: 'Mittweida'},
	{src: 'DED1A', dest: 'DED42', name: 'Stollberg'},
	{src: 'DED1B', dest: 'DED42', name: 'Aue-Schwarzenberg'},
	{src: 'DED1C', dest: 'DED45', name: 'Zwickauer Land'},
	{src: 'DED22', dest: 'DED2D', name: 'Görlitz, Kreisfreie Stadt'},
	{src: 'DED23', dest: 'DED2C', name: 'Hoyerswerda, Kreisfreie Stadt'},
	{src: 'DED24', dest: 'DED2C', name: 'Bautzen'},
	{src: 'DED25', dest: 'DED2E', name: 'Meißen'},
	{src: 'DED26', dest: 'DED2D', name: 'Niederschlesischer Oberlausitzkreis'},
	{src: 'DED27', dest: 'DED2E', name: 'Riesa-Großenhain'},
	{src: 'DED28', dest: 'DED2D', name: 'Löbau-Zittau'},
	{src: 'DED29', dest: 'DED2F', name: 'Sächsische Schweiz'},
	{src: 'DED2A', dest: 'DED2F', name: 'Weißeritzkreis'},
	{src: 'DED2B', dest: 'DED2C', name: 'Kamenz'},
	{src: 'DED31', dest: 'DED51', name: 'Leipzig, Kreisfreie Stadt'},
	{src: 'DED32', dest: 'DED53', name: 'Delitzsch'},
	{src: 'DED33', dest: 'DED43', name: 'Döbeln'},
	{src: 'DED34', dest: 'DED52', name: 'Leipziger Land'},
	{src: 'DED35', dest: 'DED52', name: 'Muldentalkreis'},
	{src: 'DED36', dest: 'DED53', name: 'Torgau-Oschatz'},

	{src: 'DE42', dest: 'DE40', name: 'BRANDENBURG SUEDWEST'},
	{src: 'DE41', dest: 'DE40', name: 'BRANDENBURG SUEDWEST'},
	{src: 'DED3', dest: 'DED5', name: 'Leipzig'},
	{src: 'DED1', dest: 'DED4', name: 'Chemnitz'}
];


const nuts_slovenia = [
	// https://www.wti-frankfurt.de/images/downloads/rechercheunterstuetzung/TED-Nuts-Codes.pdf
	// https://en.wikipedia.org/wiki/NUTS_statistical_regions_of_Slovenia
	{src: 'SI010', dest: 'SI03', name: 'Vzhodna Slovenija'},
	{src: 'SI011', dest: 'SI031', name: 'Pomurska'},
	{src: 'SI012', dest: 'SI032', name: 'Podravska'},
	{src: 'SI013', dest: 'SI033', name: 'Koroska'},
	{src: 'SI014', dest: 'SI034', name: 'Savinjska'},
	{src: 'SI015', dest: 'SI035', name: 'Zasavska'},
	{src: 'SI016', dest: 'SI036', name: 'Spodnjeposavska'},
	{src: 'SI017', dest: 'SI037', name: 'Jugovzhodna Slovenija'},
	{src: 'SI018', dest: 'SI038', name: 'Notranjsko - kraska'},
	{src: 'SI020', dest: 'SI04', name: 'Zahodna Slovenija'},
	{src: 'SI021', dest: 'SI041', name: 'Osrednjeslovenska'},
	{src: 'SI022', dest: 'SI042', name: 'Gorenjska'},
	{src: 'SI023', dest: 'SI043', name: 'Goriska'},
	{src: 'SI024', dest: 'SI044', name: 'Obalno - kraska'},
	{src: 'SI01', dest: 'SI03', name: 'Vzhodna Slovenija'},
	{src: 'SI02', dest: 'SI04', name: 'Zahodna Slovenija'},
	{src: 'SI001', dest: 'SI031', name: 'Pomurska'},
	{src: 'SI002', dest: 'SI032', name: 'Podravska'},
	{src: 'SI003', dest: 'SI033', name: 'Koroska'},
	{src: 'SI004', dest: 'SI034', name: 'Savinjska'},
	{src: 'SI005', dest: 'SI035', name: 'Zasavska'},
	{src: 'SI006', dest: 'SI036', name: 'Spodnjeposavska'},
	{src: 'SI009', dest: 'SI042', name: 'Gorenjska'},
	{src: 'SI00A', dest: 'SI044', name: 'Obalno - kraska'},
	{src: 'SI00B', dest: 'SI043', name: 'Goriska'},
	{src: 'SI00C', dest: 'SI048', name: 'Notranjsko - kraska'},
	{src: 'SI00D', dest: 'SI047', name: 'Jugovzhodna Slovenija'},
	{src: 'SI00E', dest: 'SI041', name: 'Osrednjeslovenska'}
];

let nuts_maps = [nuts_croatia, nuts_slovenia, nuts_poland, nuts_greece, nuts_italy, nuts_netherlands, nuts_uk, nuts_finland, nuts_germany, nuts_france, nuts_portugal];

let NUTSMapping = [];
nuts_maps.forEach(mapping => {
	NUTSMapping = NUTSMapping.concat(mapping);
});

class TenderConverter {

	constructor(stats, library) {
		this.stats = stats;
		this.library = library;
		if (this.stats) {
			this.stats.nuts = this.stats.nuts || {count: 0, unknown: {}, known: {}, mapped: {}};
			this.stats.body = this.stats.body || {count: 0, noname: 0};
		}
	}

	cleanNUTS(nuts) {
		let result = (nuts || []).filter(nut => nut !== null).map(n => {
			let nut = n.split('-')[0].trim();
			let map = NUTSMapping.find(m => nut === m.src);
			if (map) {
				if (this.stats) {
					this.stats.nuts.mapped[nut] = (this.stats.nuts.mapped[nut] || 0) + 1;
				}
				nut = map.dest;
			}
			if (this.stats) {
				this.stats.nuts.count = this.stats.nuts.count + 1;
				if (!this.library.isKnownNUTSCode(nut)) {
					this.stats.nuts.unknown[nut] = (this.stats.nuts.unknown[nut] || 0) + 1;
				} else {
					this.stats.nuts.known[nut] = (this.stats.nuts.known[nut] || 0) + 1;
				}
			}
			return nut;
		}).filter(nut => nut.length > 0);
		if (result.length === 0) {
			return undefined;
		}
		return result;
	};

	shortenProperties(names, o, amount) {
		names.forEach(s => {
			if (o[s] && o[s].length > amount) {
				o[s] = o[s].slice(0, amount);
			}
		});
	};

	cleanProperties(names, o) {
		names.forEach(s => {
			if (o[s]) {
				o[s] = undefined;
			}
		});
	};

	cleanGroupID(id) {
		if (id.indexOf('group_') === 0) {
			return id.slice(6);
		}
		console.log('ALARM', 'what about the freakin groupId', id);
		return id;
	};

	cleanList(list) {
		if (list) {
			let result = list.filter(doc => {
				return doc && Object.keys(doc).length > 0;
			});
			if (result.length > 0) {
				return result;
			}
		}
		return undefined;
	};

	cleanIndicators(list) {
		list = this.cleanList(list);
		if (list) {
			list.forEach(doc => {
				this.cleanProperties(['@class', 'id', 'relatedEntityId', 'created', 'modified', 'createdBy', 'modifiedBy', 'createdByVersion', 'modifiedByVersion', 'metaData'], doc);
			});
		}
		return list;
	};

	calculateIndicatorScores(list) {
		let result = [];
		let indicators = {
			CORRUPTION: [],
			TRANSPARENCY: [],
			ADMINISTRATIVE: [],
			TENDER: []
		};
		if (list) {
			list.forEach(doc => {
				if (doc.status === 'CALCULATED' && !isNaN(doc.value)) {
					indicators.TENDER.push(doc);
					Object.keys(indicators).forEach(key => {
						if (doc.type.indexOf(key) === 0) {
							indicators[key].push(doc);
						}
					});
				}
			});
		}
		Object.keys(indicators).forEach(key => {
			let l = indicators[key];
			if (l.length === 0) {
				result.push({type: key, status: 'INSUFFICIENT_DATA'});
			} else {
				let sum = 0;
				l.forEach(doc => {
					sum += doc.value;
				});
				let avg = sum / l.length;
				// console.log('avg', avg);
				result.push({type: key, value: avg, status: 'CALCULATED'});
			}
		});
		return result;
	};

	cleanBids(list) {
		list = this.cleanList(list);
		if (list) {
			list.forEach(doc => {
				this.cleanProperties(['sourceBidIds'], doc);
				doc.bidders = this.cleanBodies(doc.bidders);
				doc.unitPrices = this.cleanList(doc.unitPrices);
				doc.subcontractedValue = this.cleanPrice(doc.subcontractedValue);
				doc.price = this.cleanPrice(doc.price);
			});
		}
		return list;
	};


	cleanLots(list) {
		list = this.cleanList(list);
		if (list) {
			list.forEach(doc => {
				if (doc.title === doc.description) {
					doc.description = undefined;
				}
				this.cleanProperties(['sourceLotIds'], doc);
				this.shortenProperties(['title'], doc, 4000);
				doc.fundings = this.cleanList(doc.fundings);
				doc.bids = this.cleanBids(doc.bids);
				doc.estimatedPrice = this.cleanPrice(doc.estimatedPrice);
			});
		}
		return list;
	};


	cleanAddress(doc) {
		if (doc) {
			doc.nuts = this.cleanNUTS(doc.nuts);
		}
	};

	cleanPublications(list) {
		list = this.cleanList(list);
		if (list) {
			list.forEach(doc => {
				this.cleanProperties(['@class'], doc);
				this.shortenProperties(['buyerAssignedId'], doc, 200);
			});
		}
		return list;
	};


	cleanPrice(doc) {
		if (!doc) {
			return undefined;
		}
		this.cleanProperties(['publicationDate'], doc);
		if ((doc['netAmountEur'] > 1000000000000)) { //ignore prices larger than 1 trillion
			if (this.stats) {
				this.stats.ignored_prices = this.stats.ignored_prices || [];
				this.stats.ignored_prices.push(doc['netAmountEur']);
			}
			return undefined;
		}
		return doc;
	};

	cleanBody(doc) {
		if (!doc) {
			return undefined;
		}
		doc.id = this.cleanGroupID(doc.groupId);
		this.cleanProperties(['groupId', 'created', 'modified', 'createdBy', 'createdByVersion', 'modifiedBy', 'modifiedByVersion', '_id', 'bodyIds'], doc);
		if (this.stats) {
			this.stats.body.count = this.stats.body.count + 1;
			if (doc.name === undefined || doc.name === '') {
				this.stats.body.noname = this.stats.body.noname + 1;
			}
		}
		this.cleanAddress(doc.address);
	};

	cleanBodies(list) {
		list = this.cleanList(list);
		if (list) {
			list.forEach(doc => {
				this.cleanBody(doc);
			});
		}
		return list;
	};

	cleanItem(doc) {
		this.cleanProperties(['createdBy', 'createdByVersion', 'modifiedBy', 'modifiedByVersion'], doc);
		doc.lots = this.cleanLots(doc.lots);
		doc.specificationsProvider = this.cleanBody(doc.specificationsProvider);
		doc.furtherInformationProvider = this.cleanBody(doc.furtherInformationProvider);
		doc.bidsRecipient = this.cleanBody(doc.bidsRecipient);
		doc.buyers = this.cleanBodies(doc.buyers);
		doc.administrators = this.cleanBodies(doc.administrators);
		doc.onBehalfOf = this.cleanBodies(doc.onBehalfOf);
		doc.documents = this.cleanList(doc.documents);
		doc.publications = this.cleanPublications(doc.publications);
		doc.awardCriteria = this.cleanList(doc.awardCriteria);
		doc.cpvs = this.cleanList(doc.cpvs);
		doc.fundings = this.cleanList(doc.fundings);
		doc.documentsPrice = this.cleanPrice(doc.documentsPrice);
		doc.estimatedPrice = this.cleanPrice(doc.estimatedPrice);
		doc.finalPrice = this.cleanPrice(doc.finalPrice);
		doc.indicators = this.cleanIndicators(doc.indicators);
		doc.scores = this.calculateIndicatorScores(doc.indicators);
		this.shortenProperties(['buyerAssignedId'], doc, 200);
		return JSON.parse(JSON.stringify(doc)); //remove "undefined" properties
	};

	transform(items) {
		return items.map(item => {
			return this.cleanItem(item);
		});
	};
}

module.exports = TenderConverter;
