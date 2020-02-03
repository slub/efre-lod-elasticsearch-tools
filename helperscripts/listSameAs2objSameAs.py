#!/usr/bin/env python3

import sys
import json
from es2json import eprint

auth_provider = {
        "swb.bsz-bw.de": {
            "slubID": "https://data.slub-dresden.de/organizations/103302212",
            "abbr":"KXP",
            "preferredName":"K10Plus",
            "isBasedOn":"sourceRecord"
            },
        "d-nb.info": {
            "slubID": "https://data.slub-dresden.de/organizations/514366265",
            "abbr":"DNB",
            "preferredName":"Deutsche Nationalbibliothek",
            "isBasedOn":"sourceRecord"
            },
        "viaf.org": {
            "slubID": "https://data.slub-dresden.de/organizations/100092306",
            "abbr": "VIAF",
            "preferredName": "Virtual International Authority File (VIAF)",
            "isBasedOn": "entityFacts"
            },
        "isni.org": {
            "abbr": "ISNI",
            "preferredName": "International Standard Name Identifier (ISNI)",
            "isBasedOn": "entityFacts"
            },
        "wikidata.org": {
            "abbr": "WIKIDATA",
            "preferredName": "Wikidata",
            "slubID": "https://www.wikidata.org/wiki/Q2013",
            "isBasedOn": "sameAs"
            },
        "id.loc.gov": {
            "abbr": "LC",
            "preferredName": "NACO Authority File",
            "isBasedOn": "entityFacts",
            "slubID": "https://data.slub-dresden.de/organizations/100822142"
            },
        "deutsche-digitale-bibliothek": {
            "abbr": "DDB",
            "preferredName": "Deutsche Digitale Bibliothek",
            "isBasedOn": "entityFacts",
            "slubID": "https://data.slub-dresden.de/organizations/824631854"
            },
        "catalogue.bnf.fr": {
            "abbr": "BNF",
            "preferredName": "Bibliothèque nationale de France",
            "isBasedOn": "entityFacts",
            "slubID": "https://data.slub-dresden.de/organizations/188898441"
            },
        "de.wikipedia.org": {
            "abbr": "dewiki",
            "preferredName": "Wikipedia (Deutsch)",
            "isBasedOn": "entityFacts"
            },
        "en.wikipedia.org": {
            "abbr": "enwiki",
            "preferredName": "Wikipedia (Englisch)",
            "isBasedOn": "entityFacts"
            },
        "kalliope-verbund.info":{
            "abbr": "DE-611",
            "preferredName": "kalliope Verbundkatalog",
            "isBasedOn": "entityFacts",
            "slubID": "https://data.slub-dresden.de/organizations/103675612"
            },
        "orcid.org":{
            "abbr": "ORCID",
            "preferredName": "ORCID",
            "isBasedOn": "entityFacts",
            },
        "portraitindex.de":{
            "abbr":"Portraitindex",
            "preferredName":"Digitaler Portraitindex der druckgraphischen Bildnisse der Frühen Neuzeit",
            "isBasedOn": "entityFacts"
                },
        "archivportal-d.de":{
            "abbr":"ARCHIV-D	",
            "preferredName":"Archivportal-D	",
            "isBasedOn": "entityFacts"
                },
        "bmlo.de/Q/GND":{
            "abbr":"DE-M512",
            "preferredName":"Bayerisches Musiker-Lexikon Online",
            "isBasedOn": "entityFacts"
                },
        "#adbcontent":{
            "abbr":"ADB",
            "preferredName":"Allgemeine Deutsche Biographie (ADB)",
            "isBasedOn": "entityFacts"
                },
        
        "#ndbcontent":{
            "abbr":"NDB",
            "preferredName":"Neue Deutsche Biographie (NDB)",
            "isBasedOn": "entityFacts"
                },
        "biographien.ac.at":{
            "abbr":"OEBL",
            "preferredName":"Österreichisches Biographisches Lexikon",
            "slubID": "https://data.slub-dresden.de/organizations/102972389",
            "isBasedOn": "entityFacts"
                },
        "hls-dhs-dss.ch":{
            "abbr":"CH_HLS",
            "preferredName":"Historisches Lexikon der Schweiz (HLS)",
            "isBasedOn": "entityFacts"
                },
        "lagis-hessen.de":{
            "abbr":"LAGIS",
            "preferredName":"Hessisches Landesamt für Geschichtliche Landeskunde",
            "slubID": "https://data.slub-dresden.de/organizations/1004826004",
            "isBasedOn": "entityFacts"
                },
        "wikisource.org":{
            "abbr":"WIKISOURCE",
            "preferredName":"Wikimedia Foundation Inc.",
            "isBasedOn": "entityFacts"
                },
        "uni-rostock.de":{
            "abbr":"DE-28",
            "preferredName":"Catalogus Professorum Rostochiensium (CPR)",
            "slubID": "https://data.slub-dresden.de/organizations/100874770",
            "isBasedOn": "entityFacts"
                },
        "kulturportal-west-ost.eu":{
            "abbr":"OSTDEBIB",
            "preferredName":"Ostdeutsche Biographie",
            "isBasedOn": "entityFacts"
                },
        "pacelli-edition.de/gnd":{
            "abbr":"PACELLI",
            "preferredName":"Kritische Online-Edition der Nuntiaturberichte Eugenio Pacellis (1917-1929)",
            "isBasedOn": "entityFacts"
                },
        "frankfurter-personenlexikon.de":{
            "abbr":"FFMPL",
            "preferredName":"Frankfurter Personenlexikon",
            "slubID": "https://data.slub-dresden.de/organizations/236770764",
            "isBasedOn": "entityFacts"
                },
        "steinheim-institut.de":{
            "abbr":"epidat",
            "preferredName":"epigraphische Datenbank",
            "slubID": "https://data.slub-dresden.de/organizations/103039031",
            "isBasedOn": "entityFacts"
                },
        "www.historicum.net":{
            "abbr":"HISTORICUM",
            "preferredName":"Klassiker der Geschichtswissenschaft",
            "slubID": "https://data.slub-dresden.de/organizations/102398704",
            "isBasedOn": "entityFacts"
                },
        "uni-graz.at":{
            "abbr":"BIOKLASOZ",
            "preferredName":"Biographie und Bibliographie bei 50 Klassiker der Soziologie",
            "slubID": "https://data.slub-dresden.de/organizations/100832873",
            "isBasedOn": "entityFacts"
                },
        "filmportal.de":{
            "abbr":"filmportal.de",
            "preferredName":"filmportal.de",
            "isBasedOn": "entityFacts"
                },
        "geonames.org":{
            "abbr": "geonames",
            "preferredName": "GeoNames",
            "isBasedOn": "sameAs"
        }
        
    }

for line in sys.stdin:
    rec = json.loads(line)
    if rec.get("sameAs") and isinstance(rec["sameAs"], list):
        sameAsses = rec.pop("sameAs")
        rec["sameAs"] = []
        for sameAs in sameAsses:
            obj=None
            for ap,value in auth_provider.items():
                if ap in sameAs:
                    obj = {
                        '@id': sameAs,
                        'publisher': {
                            'abbr': value["abbr"],
                            'preferredName': value["preferredName"]
                        },
                        "isBasedOn": {
                            "@type": "Dataset"
                        }
                    }
                    if "slubID" in value:
                        obj["publisher"]["@id"]=value["slubID"]
                    if "sourceRecord" in value["isBasedOn"]:
                        obj["isBasedOn"]["@id"]=rec["isBasedOn"]
                    elif "entityFacts" in value["isBasedOn"]:
                        for sameAs in sameAsses:
                            if "d-nb.info" in sameAs:
                                obj["isBasedOn"]["@id"]="http://hub.culturegraph.org/entityfacts/{}".format(sameAs.split("/")[-1])
                                break
                    elif "sameAs" in value["isBasedOn"]:
                        obj["isBasedOn"]["@id"]=obj["@id"]
            if isinstance(obj,dict):
                rec["sameAs"].append(obj)
            elif obj:
                eprint(obj)
            else:
                eprint(sameAs)
    rec["preferredName"]=rec.pop("name")
    print(json.dumps(rec))
        
            
