#!/usr/bin/python3

import argparse
import json
import sys
import requests
from es2json import esgenerator, isint, litter


lookup_table_wdProperty = {"http://viaf.org": {"property": "P214",
                                               "delim": "/"},
                           "https://d-nb.info/gnd": {"property": "P227",
                                                     "delim": "/"},
                           "http://isni.org": {"property": "P213",
                                               "delim": "/"},
                           "http://id.loc.gov": {"property": "P244",
                                                 "delim": "/"},
                           "https://deutsche-digitale-bibliothek.de": {"property": "P4948",
                                                                       "delim": "/"},
                           "http://catalogue.bnf.fr/ark": {"property": "P268",
                                                           "delim": "/cb"},
                           "http://geonames.org": {"property": "P1566",
                                                   "delim": "/"},
                           "http://filmportal.de/person": {"property": "P2639",
                                                           "delim": "/"},
                           "http://orcid.org": {"property": "P496",
                                                "delim": "/"},
                           "http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN=": {"property": "P1044",
                                                                       "delim": "="}
                           }


def get_wdid(_ids, rec):
    """
    gets an list of sameAs Links, e.g. ['https://d-nb.info/gnd/118827545', 'http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN=035143010', 'http://catalogue.bnf.fr/ark:/12148/cb119027159', 'http://id.loc.gov/rwo/agents/n50002729', 'http://isni.org/isni/0000000120960218', 'http://viaf.org/viaf/44298691']
    """
    if not isinstance(_ids, list):
        return None
    changed = False
    url = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"

    or_mapping = []
    for _id in _ids:
        for key, value in lookup_table_wdProperty.items():
            if _id.startswith(key):
                or_mapping.append("?item wdt:{Property} \"{value}\"".format(
                    Property=value["property"], value=_id.split(value["delim"])[-1]))
                break

    if or_mapping:
        # BUILD an SPARQL OR Query with an UNION Operator.
        # Still builds an normal query without UNION when or_mapping List only contains one element
        query = '''SELECT DISTINCT ?item \nWHERE {{\n\t{{ {UNION} }}\n}}'''.format(
            UNION="} UNION\n\t\t {".join(or_mapping))
        data = requests.get(url, params={'query': query, 'format': 'json'})
        if data.ok and len(data.json().get("results").get("bindings")) > 0:
            for item in data.json().get("results").get("bindings"):
                rec["sameAs"] = litter(
                    rec["sameAs"], {"@id": item.get("item").get("value"),
                                    "publisher": {
                        "@id": "https://www.wikidata.org/wiki/Q2013",
                        "abbr": "WIKIDATA",
                        "name": "Wikidata"},
                        "isBasedOn": {
                            "@type": "Dataset",
                            "@id": item.get("item").get("value")}})
                changed = True
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
            record = None
            if rec and isinstance(rec.get("sameAs"), list):
                record = get_wdid([x["@id"] for x in rec["sameAs"]], rec)
                if record:
                    rec = record
            if (record or args.pipeline) and rec:
                print(json.dumps(rec, indent=None))
    else:
        body = {"query": {"bool": {"filter": {"bool": {"should": [], "must_not": [
            {"prefix": {"sameAs.@id.keyword": "http://www.wikidata.org"}}]}}}}}
        for key in lookup_table_wdProperty:
            body["query"]["bool"]["filter"]["bool"]["should"].append(
                {"prefix": {"sameAs.@id.keyword": key}})
        for rec in esgenerator(host=args.host, port=args.port, index=args.index, type=args.type, id=args.id, headless=True, body=body):
            record = None
            if rec.get("sameAs") and isinstance(rec.get("sameAs"), list):
                record = get_wdid([x["@id"] for x in rec["sameAs"]], rec)
                if record:
                    rec = record
            if record or args.pipeline:
                print(json.dumps(rec, indent=None))
