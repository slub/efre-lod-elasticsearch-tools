#!/usr/bin/python3

import argparse
import sys
import json
import pymarc
import traceback

from pymarc.exceptions import RecordLengthInvalid, RecordLeaderInvalid, BaseAddressNotFound, BaseAddressInvalid, RecordDirectoryInvalid, NoFieldsFound

from multiprocessing import Pool, Lock


rolemapping = {
    "abr": "KürzendeR",
    "acp": "HerstellerIn von Nachbildungen",
    "act": "SchauspielerIn",
    "adi": "Art Director",
    "adp": "BearbeiterIn",
    "aft": "VerfasserIn eines Nachworts",
    "anl": "AnalytikerIn",
    "anm": "TrickfilmzeichnerIn",
    "ann": "KommentatorIn",
    "ant": "BibliographischeR VorgängerIn",
    "ape": "BerufungsbeklagteR/RevisionsbeklagteR",
    "apl": "BerufungsklägerIn/RevisionsklägerIn",
    "app": "AntragstellerIn",
    "aqt": "AutorIn von Zitaten oder Textabschnitten",
    "arc": "ArchitektIn",
    "ard": "künstlerische Leitung",
    "arr": "ArrangeurIn",
    "art": "KünstlerIn",
    "asg": "RechtsnachfolgerIn",
    "asn": "zugehöriger Name",
    "ato": "AutographIn",
    "att": "zugehöriger Name",
    "auc": "AuktionatorIn",
    "aud": "AutorIn des Dialogs",
    "aui": "VerfasserIn eines Geleitwortes",
    "aus": "DrehbuchautorIn",
    "aut": "VerfasserIn",
    "bdd": "BindungsgestalterIn",
    "bjd": "EinbandgestalterIn",
    "bkd": "BuchgestalterIn",
    "bkp": "BuchherstellerIn",
    "blw": "AutorIn des Klappentextes",
    "bnd": "BuchbinderIn",
    "bpd": "GestalterIn des Exlibris",
    "brd": "Sender",
    "brl": "BrailleschriftprägerIn",
    "bsl": "BuchhändlerIn",
    "cas": "FormgießerIn",
    "ccp": "konzeptionelle Leitung",
    "chr": "ChoreografIn",
    "clb": "MitarbeiterIn",
    "cli": "KlientIn, AuftraggeberIn",
    "cll": "KalligrafIn",
    "clr": "KoloristIn",
    "clt": "LichtdruckerIn",
    "cmm": "KommentatorIn",
    "cmp": "KomponistIn",
    "cmt": "SchriftsetzerIn",
    "cnd": "DirigentIn",
    "cng": "Kameramann/frau",
    "cns": "ZensorIn",
    "coe": "BerufungsbeklagteR im streitigen Verfahren",
    "col": "SammlerIn",
    "com": "ZusammenstellendeR",
    "con": "KonservatorIn",
    "cor": "SammlungskuratorIn",
    "cos": "AnfechtendeR, bestreitende Partei",
    "cot": "BerufungsklägerIn im streitigen Verfahren",
    "cou": "zuständiges Gericht",
    "cov": "UmschlaggestalterIn",
    "cpc": "BeansprucherIn des Urheberrechts",
    "cpe": "BeschwerdeführerIn-BerufungsbeklagteR",
    "cph": "InhaberIn des Urheberrechts",
    "cpl": "BeschwerdeführerIn/KlägerIn",
    "cpt": "KlägerIn/BerufungsklägerIn",
    "cre": "GeistigeR SchöpferIn",
    "crp": "KorrespondentIn",
    "crr": "KorrektorIn",
    "crt": "GerichtsstenografIn",
    "csl": "BeraterIn",
    "csp": "ProjektberaterIn",
    "cst": "KostümbildnerIn",
    "ctb": "MitwirkendeR",
    "cte": "AnfechtungsgegnerIn-BerufungsbeklagteR",
    "ctg": "KartografIn",
    "ctr": "VertragspartnerIn",
    "cts": "AnfechtungsgegnerIn",
    "ctt": "AnfechtungsgegnerIn-BerufungsklägerIn",
    "cur": "KuratorIn",
    "cwt": "KommentatorIn",
    "dbp": "Erscheinungsort",
    "dfd": "AngeklagteR/BeklagteR",
    "dfe": "AngeklagteR/BeklagteR-BerufungsbeklagteR",
    "dft": "AngeklagteR/BeklagteR-BerufungsklägerIn",
    "dgg": "Grad-verleihende Institution",
    "dgs": "AkademischeR BetreuerIn",
    "dir": "Dirigent",
    "dis": "PromovierendeR",
    "dln": "VorzeichnerIn",
    "dnc": "TänzerIn",
    "dnr": "GeldgeberIn",
    "dpc": "AbgebildeteR",
    "dpt": "AnlegerIn",
    "drm": "TechnischeR ZeichnerIn",
    "drt": "RegisseurIn",
    "dsr": "DesignerIn",
    "dst": "Vertrieb",
    "dtc": "BereitstellerIn von Daten",
    "dte": "WidmungsempfängerIn",
    "dtm": "DatenmanagerIn",
    "dto": "WidmendeR",
    "dub": "angeblicheR AutorIn",
    "edc": "BearbeiterIn der Zusammenstellung",
    "edm": "CutterIn",
    "edt": "HerausgeberIn",
    "egr": "StecherIn",
    "elg": "ElektrikerIn",
    "elt": "GalvanisiererIn",
    "eng": "IngenieurIn",
    "enj": "Normerlassende Gebietskörperschaft",
    "etr": "RadiererIn",
    "evp": "Veranstaltungsort",
    "exp": "ExperteIn",
    "fac": "FacsimilistIn",
    "fds": "Filmvertrieb",
    "fld": "BereichsleiterIn",
    "flm": "BearbeiterIn des Films",
    "fmd": "FilmregisseurIn",
    "fmk": "FilmemacherIn",
    "fmo": "frühereR BesitzerIn",
    "fmp": "FilmproduzentIn",
    "fnd": "GründerIn",
    "fpy": "Erste Partei",
    "frg": "FälscherIn",
    "gis": "GeographIn",
    "grt": "GraphischeR TechnikerIn",
    "hg": "Herausgeber",
    "his": "Gastgebende Institution",
    "hnr": "GefeierteR",
    "hst": "GastgeberIn",
    "Ill": "Illustrator",
    "ill": "IllustratorIn",
    "ilu": "Illuminator, BuchmalerIn",
    "ins": "InserierendeR",
    "inv": "ErfinderIn",
    "isb": "Herausgebendes Organ",
    "itr": "InstrumentalmusikerIn",
    "ive": "InterviewteR",
    "ivr": "InterviewerIn",
    "jud": "RichterIn",
    "jug": "zuständige Gerichtsbarkeit",
    "kad": "Kadenzverfasser",
    "lbr": "Labor",
    "lbt": "LibrettistIn",
    "ldr": "Laborleitung",
    "led": "Führung",
    "lee": "Libelee-appellee",
    "lel": "BeklagteR im Seerecht/Kirchenrecht",
    "len": "LeihgeberIn",
    "let": "Libelee-appellant",
    "lgd": "LichtgestalterIn",
    "lie": "Libelant-appellee",
    "lil": "KlägerIn im Seerecht/Kirchenrecht",
    "lit": "Libelant-appellant",
    "lsa": "LandschaftsarchitektIn",
    "lse": "LizenznehmerIn",
    "lso": "LizenzgeberIn",
    "ltg": "LithographIn",
    "lyr": "TextdichterIn",
    "mcp": "ArrangeurIn, Notenleser/-schreiberIn",
    "mdc": "Metadatenkontakt",
    "med": "Medium",
    "mfp": "Herstellungsort",
    "mfr": "HerstellerIn",
    "mod": "ModeratorIn",
    "mon": "BeobachterIn",
    "mrb": "MarmorarbeiterIn, MarmoriererIn",
    "mrk": "Markup-EditorIn",
    "msd": "MusikalischeR LeiterIn",
    "mte": "Metall-GraveurIn",
    "mtk": "ProtokollantIn",
    "mus": "MusikerIn",
    "nrt": "ErzählerIn",
    "opn": "GegnerIn",
    "org": "UrheberIn",
    "orm": "VeranstalterIn",
    "osp": "On-screen PräsentatorIn",
    "oth": "BerichterstatterIn",
    "own": "BesitzerIn",
    "pan": "DiskussionsteilnehmerIn",
    "pat": "SchirmherrIn",
    "pbd": "Verlagsleitung",
    "pbl": "Verlag",
    "pdr": "Projektleitung",
    "pfr": "Korrektur",
    "pht": "FotografIn",
    "plt": "DruckformherstellerIn",
    "pma": "Genehmigungsstelle",
    "pmn": "Produktionsleitung",
    "pop": "PlattendruckerIn",
    "ppm": "PapiermacherIn",
    "ppt": "PuppenspielerIn",
    "pra": "Praeses",
    "prc": "Prozesskontakt",
    "prd": "Produktionspersonal",
    "pre": "PräsentatorIn",
    "prf": "AusführendeR",
    "prg": "ProgrammiererIn",
    "prm": "DruckgrafikerIn",
    "prn": "Produktionsfirma",
    "pro": "ProduzentIn",
    "prp": "Produktionsort",
    "prs": "SzenenbildnerIn",
    "prt": "DruckerIn",
    "prv": "AnbieterIn",
    "pta": "PatentanwärterIn",
    "pte": "KlägerIn-BerufungsbeklagteR",
    "ptf": "ZivilklägerIn",
    "pth": "PatentinhaberIn",
    "ptt": "KlägerIn-BerufungsklägerIn",
    "pup": "Veröffentlichungsort",
    "rbr": "RubrikatorIn",
    "rcd": "TonmeisterIn",
    "rce": "ToningenieurIn",
    "rcp": "AdressatIn",
    "rdd": "HörfunkregisseurIn",
    "Red": "Redakteur",
    "red": "RedakteurIn",
    "ren": "RendererIn (Bildverarbeitung)",
    "res": "ForscherIn",
    "rev": "RezensentIn, GutachterIn",
    "rpc": "HörfunkproduzentIn",
    "rps": "Aufbewahrungsort, TreuhänderIn",
    "rpt": "ReporterIn",
    "rpy": "Verantwortliche Partei",
    "rse": "AntragsgegnerIn-BerufungsbeklagteR",
    "rsg": "RegisseurIn der Wiederaufführung",
    "rsp": "RespondentIn",
    "rsr": "RestauratorIn",
    "rst": "AntragsgegnerIn-BerufungsklägerIn",
    "rth": "Leitung des Forschungsteams",
    "rtm": "Mitglied des Forschungsteams",
    "sad": "WissenschaftlicheR BeraterIn",
    "sce": "DrehbuchautorIn",
    "scl": "BildhauerIn",
    "scr": "SchreiberIn",
    "sds": "Tongestalter",
    "sec": "SekretärIn",
    "sgd": "BühnenregisseurIn",
    "sgn": "UnterzeichnerIn",
    "sht": "Unterstützender Veranstalter",
    "sll": "VerkäuferIn",
    "sng": "SängerIn",
    "spk": "RednerIn",
    "spn": "SponsorIn",
    "spy": "Zweite Partei",
    "srv": "LandvermesserIn",
    "std": "BühnenbildnerIn",
    "stg": "Kulisse",
    "stl": "GeschichtenerzählerIn",
    "stm": "InszenatorIn",
    "stn": "Normungsorganisation",
    "str": "StereotypeurIn",
    "tcd": "Technische Leitung",
    "tch": "LehrerIn",
    "ths": "BetreuerIn (Doktorarbeit)",
    "tld": "FernsehregisseurIn",
    "tlp": "FernsehproduzentIn",
    "trc": "TranskribiererIn",
    "trl": "ÜbersetzerIn",
    "tyd": "Schrift-DesignerIn",
    "tyg": "SchriftsetzerIn",
    "uvp": "Hochschulort",
    "vac": "SynchronsprecherIn",
    "vdg": "BildregisseurIn",
    "voc": "VokalistIn",
    "wac": "KommentarverfasserIn",
    "wal": "VerfasserIn von zusätzlichen Lyrics",
    "wam": "AutorIn des Begleitmaterials",
    "wat": "VerfasserIn von Zusatztexten",
    "wdc": "HolzschneiderIn",
    "wde": "HolzschnitzerIn",
    "win": "VerfasserIn einer Einleitung",
    "wit": "ZeugeIn",
    "wpr": "VerfasserIn eines Vorworts",
    "wst": "VerfasserIn von ergänzendem Text"
}

baseuri = "http://data.finc.info/resources/"


prop2isil = {"swb_id_str": "(DE-576)",
             "kxp_id_str": "(DE-627)"
             }


def fixRecord(record="", record_id=0, validation=False, replaceMethod='decimal'):
        replaceMethods = {
            'decimal': (('#29;', '#30;', '#31;'), ("\x1D", "\x1E", "\x1F")),
            'unicode': (('\u001d', '\u001e', '\u001f'), ("\x1D", "\x1E", "\x1F")),
            'hex': (('\x1D', '\x1E', '\x1F'), ("\x1D", "\x1E", "\x1F"))
        }
        marcFullRecordFixed=record
        for i in range(0, 3):
            marcFullRecordFixed=marcFullRecordFixed.replace(replaceMethods.get(replaceMethod)[0][i], replaceMethods.get(replaceMethod)[1][i])
        if validation:
            try:
                reader = pymarc.MARCReader(marcFullRecordFixed.encode('utf8'), utf8_handling='replace')
                marcrecord = next(reader)
            except (RecordLengthInvalid, RecordLeaderInvalid, BaseAddressNotFound, BaseAddressInvalid, RecordDirectoryInvalid, NoFieldsFound, UnicodeDecodeError) as e:
                eprint("record id {0}:".format(record_id)+str(e))
                with open('invalid_records.txt', 'a') as error:
                    eprint(marcFullRecordFixed,file=error)
                    return None
        return marcFullRecordFixed


def ArrayOrSingleValue(array):
    '''
    return an array
    if there is only a single value, only return that single value
    '''
    if isinstance(array, (int, float)):
        return array
    if array:
        length = len(array)
        if length > 1 or isinstance(array, dict):
            return array
        elif length == 1:
            for elem in array:
                return elem
        elif length == 0:
            return None


def eprint(*args, **kwargs):
    '''
    print to stderr
    '''
    print(*args, file=sys.stderr, **kwargs)


def getIDs(record, prop):
    if isinstance(prop, str):
        if prop in prop2isil and prop in record:
            return str(prop2isil[prop]+record[prop])
        elif prop in record and not prop in prop2isil:
            return str(record[prop])
    elif isinstance(prop, list):
        ret = []
        for elem in prop:
            if elem in prop2isil and elem in record:
                ret.append(str(prop2isil[elem]+record[elem]))
            elif elem in record and not elem in prop2isil:
                ret.append(record[elem])
        if ret:
            return ret


def getoAC(record, prop):
    if isinstance(record.get(prop), str):
        if record.get(prop) == "Free":
            return "Yes"
    elif isinstance(record.get(prop), list):
        for elem in record.get(prop):
            if elem == "Free":
                return "Yes"


def getAtID(record, prop):
    if record.get(prop):
        return baseuri+record[prop]


def getGND(record, prop):
    ret = []
    if isinstance(record.get(prop), str):
        return "http://d-nb.info/gnd/"+record.get(prop)
    elif isinstance(record.get(prop), list):
        for elem in record.get(prop):
            ret.append("http://d-nb.info/gnd/"+elem)
    if ret:
        return ret
    else:
        return None


def getLanguage(record, prop):
    lang = getProperty(record, prop)
    if lang:
        language = {"en": lang}
        return language


def getTitle(record, prop):
    title = getProperty(record, prop)
    if title:
        if isinstance(title, str):
            if title[-2:] == " /":
                title = title[:-2]
        elif isinstance(title, list):
            for n, elem in enumerate(title):
                if elem[-2:] == " /":
                    title[n] = title[n][:-2]
        return title


def getformat(record, prop, formattable):
    if isinstance(record.get(prop), str) and record.get(prop) in formattable:
        return formattable.get(record.get(prop))
    elif isinstance(record.get(prop), list):
        for elem in record.get(prop):
            if elem in formattable:
                return formattable.get(elem)


def getFormatRdfType(record, prop):
    formatmapping = {"Article, E-Article": "bibo:Article",
                     "Book, E-Book": "bibo:Book",
                     "Journal, E-Journal": "bibo:Periodical",
                     "Manuscript": "bibo:Manuscript",
                     "Map": "bibo:Map",
                     "Thesis": "bibo:Thesis",
                     "Video": "bibo:AudioVisualDocument"
                     }
    value = getformat(record, prop, formatmapping)
    if value:
        return {"@id": value}
    else:
        return {"@id": "bibo:Document"}


def getFormatDctMedium(record, prop):
    formatmapping = {"Audio": "rdamt:1001",
                     "Microform": "rdamt:1002",
                     "Notated Music": "rdau:P60488"
                     }
    value = getformat(record, prop, formatmapping)
    return value if value else None


def getOfferedBy(record, prop):
    if record.get(prop):
        return {
            "@type": "http://schema.org/Offer",
            "schema:offeredBy": {
                "@id": "https://data.finc.info/organisation/DE-15",
                "@type": "schema:Library",
                "schema:name": "Univeristätsbibliothek Leipzig",
                "schema:branchCode": "DE-15"
            },
            "schema:availability": "http://data.ub.uni-leipzig.de/item/wachtl/DE-15:ppn:"+record[prop]
        }


def getProperty(record, prop):
    ret = []
    if isinstance(prop, str):
        if prop in record:
            return record.get(prop)
    elif isinstance(prop, list):
        for elem in prop:
            if isinstance(record.get(elem), str):
                ret.append(record[elem])
            elif isinstance(record.get(elem), list):
                for elen in record[elem]:
                    ret.append(elen)
    if ret:
        return ret
    else:
        return None


def getIsPartOf(record, prop):
    data = getProperty(record, prop)
    if isinstance(data, str):
        return {"@id": "https://data.finc.info/resources/"+data}
    elif isinstance(data, list):
        ret = []
        for elem in data:
            ret.append({"@id": "https://data.finc.info/resources/"+elem})
        return ret


def getIssued(record, prop):
    data = getProperty(record, prop)
    if isinstance(data, str):
        return {context.get("dateTime"): data}
    elif isinstance(data, list):
        ret = []
        for elem in data:
            ret.append({"@type": "xsd:gYear",
                        "@value": elem})
        return ret


"""...
  "contribution" : [ {
    "type" : [ "Contribution" ],
    "agent" : {
      "id" : "http://d-nb.info/gnd/1049709292",
      "type" : [ "Person" ],
      "dateOfBirth" : "1974",
      "gndIdentifier" : "1049709292",
      "label" : "Nichols, Catherine" 
    },
    "role" : {
      "id" : "http://id.loc.gov/vocabulary/relators/edt",
      "label" : "Herausgeber/in" 
    }
  }, {
    "type" : [ "Contribution" ],
    "agent" : {
      "id" : "http://d-nb.info/gnd/130408026",
      "type" : [ "Person" ],
      "dateOfBirth" : "1951",
      "gndIdentifier" : "130408026",
      "label" : "Blume, Eugen" 
    },
    "role" : {
      "id" : "http://id.loc.gov/vocabulary/relators/ctb",
      "label" : "Mitwirkende" 
    }
  }
"""


def get_contributon(record, prop):
    fullrecord_fixed = fixRecord(record=getProperty(record, prop), record_id=record.get(
        "record_id"), validation=False, replaceMethod='decimal')
    reader = pymarc.MARCReader(fullrecord_fixed.encode('utf-8'))
    data = []
    fields = ["100", "110", "111", "700", "710", "711"]
    for record in reader:
        for field in fields:
            for f in record.get_fields(field):
                contributor = {
                    "@type": ["bf:Contribution"],
                    "bf:agent": {
                        "@id": "http://d-nb.info/gnd/"
                    },
                    "bf:role": {
                        "@id": "http://id.loc.gov/vocabulary/relators/",
                    }
                }
                if f['a']:
                    contributor["bf:agent"]["rdfs:ch_label"] = f['a']
                if f['0'] and f['0'].startswith("(DE-588)"):
                    contributor["bf:agent"]["@id"] += f['0'].split(")")[1]
                else:
                    del contributor['bf:agent']['@id']
                if f['4'] and len(f['4']) <= 4:
                    if f['4'][0] == '-':
                        contributor['bf:role']['@id'] += f['4'][1:]
                        if rolemapping.get(f['4'][1:]):
                            contributor['bf:role']['rdfs:ch_label'] = rolemapping[f['4'][1:]]
                    else:
                        contributor['bf:role']['@id'] += f['4']
                        if rolemapping.get(f['4']):
                            contributor['bf:role']['rdfs:ch_label'] = rolemapping[f['4']]
                else:
                    del contributor['bf:role']
                if field[1:] == "00":
                    contributor['bf:agent']['@type'] = 'bf:Person'
                elif field[1:] == "10":
                    contributor['bf:agent']['@type'] = 'bf:Organization'
                elif field[1:] == "11":
                    contributor['bf:agent']['@type'] = 'bf:Meeting'
                if contributor['bf:agent'].get('rdfs:ch_label'):
                    data.append(contributor)

    return data if data else None


def get_rvk(record, prop):
    if prop in record:
        for rvk in record[prop]:
            if rvk == "No subject assigned":
                continue
            elif isinstance(rvk, str):
                return str("https://rvk.uni-regensburg.de/regensburger-verbundklassifikation-online#notation/{}".format(rvk))


def putContext(record):
    return context

# mapping={ "target_field":"someString"},

#           "target_field":{function:"source_field"}}


context = {
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "bf": "http://id.loc.gov/ontologies/bibframe/",
    "dct": "http://purl.org/dc/terms/",
    "dc": "http://purl.org/dc/terms/",
    "bibo": "http://purl.org/ontology/bibo/",
    "rdau": "http://rdaregistry.info/Elements/u/",
    "umbel": "http://umbel.org/umbel/",
    "isbd": "http://iflastandards.info/ns/isbd/elements/",
    "schema": "http://schema.org/",
    "rdfs": "https://www.w3.org/TR/rdf-schema/#",
    "issued": {
        "@id": "dct:issued",
        "@type": "xsd:gYear"
    },
    "identifier": {
        "@id": "dct:identifier",
        "@type": "xsd:string"
    },
    "language": {
        "@id": "http://purl.org/dc/terms/language",
        "@container": "@language"
    },
    "openAccessContent": "http://dbpedia.org/ontology/openAccessContent",
}


mapping = {
    "@context": putContext,
    "@id": {getAtID: "id"},
    "identifier": {getIDs: ["swb_id_str", "kxp_id_str"]},
    "bibo:issn": {getProperty: "issn"},
    "bibo:isbn": {getProperty: "isbn"},
    "umbel:isLike": {getProperty: "url"},
    "dct:title": {getTitle: "title"},
    "rdau:P60493": {getTitle: ["title_part", "title_sub"]},
    "bibo:shortTitle": {getTitle: "title_short"},
    "dct:alternative": {getTitle: "title_alt"},
    "rdau:P60327": {getProperty: "author"},
    "dc:contributor": {getProperty: "author2"},
    # "author_id":{getGND:"author_id"},
    "rdau:P60333": {getProperty: "imprint_str_mv"},
    "rdau:P60163": {getProperty: "publishPlace"},
    "dct:publisher": {getProperty: "publisher"},
    "issued": {getIssued: "publishDate"},
    "rdau:P60489": {getProperty: "dissertation_note"},
    "isbd:P1053": {getProperty: "physical"},
    "language": {getLanguage: "language"},
    "dct:isPartOf": {getIsPartOf: "hierarchy_top_id"},
    "dct:bibliographicCitation": {getProperty: ["container_title", "container_reference"]},
    "rdfs:ch_type": {getFormatRdfType: "format_finc"},
    "dct:medium": {getFormatDctMedium: "format_finc"},
    "openAccessContent": {getoAC: "facet_avail"},
    "schema:offers": {getOfferedBy: "record_id"},
    "bf:contribution": {get_contributon: "fullrecord"},
    "umbel:relatesToNotation": {get_rvk: "rvk_facet"}
}


def process_field(record, source_field):
    ret = []
    if isinstance(source_field, dict):
        for function, parameter in source_field.items():
            ret.append(function(record, parameter))
    elif isinstance(source_field, str):
        return value
    elif isinstance(source_field, list):
        for elem in value:
            ret.append(ArrayOrSingleValue(process_field(record, elem)))
    elif callable(source_field):
        return ArrayOrSingleValue(source_field(record))
    if ret:
        return ArrayOrSingleValue(ret)


def removeNone(obj):
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(removeNone(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return type(obj)((removeNone(k), removeNone(v))
                         for k, v in obj.items() if k is not None and v is not None)
    else:
        return obj


lock = Lock()


def process_line(record):
    try:
        mapline = {}
        for key, val in mapping.items():
            value = process_field(record, val)
            if value:
                mapline[key] = value
        mapline = removeNone(mapline)
        if mapline:
            with lock:
                sys.stdout.write(json.dumps(mapline, indent=None)+"\n")
                sys.stdout.flush()
    except Exception as e:
        with open("errors.txt", 'a') as f:
            traceback.print_exc(file=f)


def gen_solrdump_cmd(host):
    fl = set()
    for k, v in mapping.items():
        if not callable(v):
            for c, w in v.items():
                if isinstance(w, str):
                    fl.add(w)
                elif isinstance(w, list):
                    for elem in w:
                        fl.add(elem)
    return "solrdump -verbose -server {} -fl {}".format(host, ','.join(fl))


def main():
    parser = argparse.ArgumentParser(
        description='simple LOD Mapping of FINC-Records')
    parser.add_argument('-gen_cmd', action="store_true",
                        help='generate bash command')
    parser.add_argument(
        '-server', type=str, help="which server to use for harvest, only used for cmd prompt definition")
    args = parser.parse_args()
    if args.gen_cmd:
        print(gen_solrdump_cmd(args.server))
        quit()
    p = Pool(4)
    for line in sys.stdin:
        p.apply_async(process_line, args=(json.loads(line),))
        #target_record=process_line(json.loads(line))
        #if target_record:
            #print(json.dumps(target_record))


if __name__ == "__main__":
    main()
