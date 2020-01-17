#!/usr/bin/python3

import argparse
import json
import sys
import requests
from es2json import esgenerator, isint, litter, eprint


lookup_table_wdProperty = {"https://d-nb.info/gnd": "P227",
                           "http://viaf.og/viaf": "P214",
                           "http://isni.org": "P213",
                           "http://id.loc.gov/authorities": "P244",
                           "https://deutsche-digitale-bibliothek.de/entity": "P4948",
                           "http://catalogue.bnf.fr/ark": "P268",
                           "http://geonames.org": "P1566",
                           "http://filmportal.de/person": "P2639",
                           "http://orcid.org": "P496"}


def get_wdid(_id, rec):
    changed = False

    url = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"
    lookup_value = _id.split("/")[-1]
    lookup_property = ""
    for key, value in lookup_table_wdProperty.items():
        if _id.startswith(key):
            lookup_property = value
    if lookup_value and lookup_property:
        query = '''
            PREFIX wikibase: <http://wikiba.se/ontology#>
            PREFIX wd: <http://www.wikidata.org/entity/>
            PREFIX wdt: <http://www.wikidata.org/prop/direct/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            
            SELECT ?person
            WHERE {{
                ?person wdt:{prop}  "{value}" .
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
            }}'''.format(prop=lookup_property, value=lookup_value)
        try:
            data = requests.get(
                url, params={'query': query, 'format': 'json'}).json()
            if len(data.get("results").get("bindings")) > 0:
                for item in data.get("results").get("bindings"):
                    rec["sameAs"] = litter(
                        rec["sameAs"], item.get("person").get("value"))
                    changed = True
        except:
            pass
    if changed:
        return rec


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='enrich ES by WD!')
    parser.add_argument('-host', type=str, default="127.0.0.1",
                        help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-port', type=int, default=9200,
                        help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index', type=str,
                        help='ElasticSearch Search Index to use')
    parser.add_argument('-type', type=str,
                        help='ElasticSearch Search Index Type to use')
    parser.add_argument(
        "-id", type=str, help="retrieve single document (optional)")
    parser.add_argument('-stdin', action="store_true",
                        help="get data from stdin")
    parser.add_argument('-pipeline', action="store_true",
                        help="output every record (even if not enriched) to put this script into a pipeline")
    # no, i don't steal the syntax from esbulk...
    parser.add_argument(
        '-server', type=str, help="use http://host:port/index/type/id?pretty. overwrites host/port/index/id/pretty")
    args = parser.parse_args()
    if args.server:
        slashsplit = args.server.split("/")
        args.host = slashsplit[2].rsplit(":")[0]
        if isint(args.server.split(":")[2].rsplit("/")[0]):
            args.port = args.server.split(":")[2].split("/")[0]
        args.index = args.server.split("/")[3]
        if len(slashsplit) > 4:
            args.type = slashsplit[4]
        if len(slashsplit) > 5:
            if "?pretty" in args.server:
                args.pretty = True
                args.id = slashsplit[5].rsplit("?")[0]
            else:
                args.id = slashsplit[5]
    if args.stdin:
        for line in sys.stdin:
            rec = json.loads(line)
            gnd = None
            record = None
            if rec and rec.get("sameAs"):
                if isinstance(rec.get("sameAs"), list):
                    for item in rec.get("sameAs"):
                        record = get_wdid(item, rec)
                        if record:
                            break
                elif isinstance(rec.get("sameAs"), str):
                    record = get_wdid(item, rec)
            if record:
                rec = record
            if (record or args.pipeline) and rec:
                print(json.dumps(rec, indent=None))
    else:
        body = {"query": {"bool": {"filter": {"bool": {"should": [], "must_not": [
            {"prefix": {"sameAs.keyword": "http://www.wikidata.org"}}]}}}}}
        for key in lookup_table_wdProperty:
            body["query"]["bool"]["filter"]["bool"]["should"].append(
                {"prefix": {"sameAs.keyword": key}})
        for rec in esgenerator(host=args.host, port=args.port, index=args.index, type=args.type, headless=True, body=body):
            record = None
            if rec.get("sameAs"):
                if isinstance(rec.get("sameAs"), list):
                    for item in rec.get("sameAs"):
                        record = get_wdid(item, rec)
                        if record:
                            break
                elif isinstance(rec.get("sameAs"), str):
                    record = get_wdid(rec.get("sameAs"), rec)
            if record:
                rec = record
            if record or args.pipeline:
                print(json.dumps(rec, indent=None))
