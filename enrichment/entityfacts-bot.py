#!/usr/bin/python3
# -*- coding: utf-8 -*-
import json
import argparse
import sys
import time
from requests import get
from es2json import esgenerator
from es2json import isint
from es2json import litter
from es2json import eprint

def entityfacts(record, gnd, ef_instances):
    abbrevations={"DNB":"https://data.slub-dresden.de/organizations/514366265",
                  "VIAF":"https://data.slub-dresden.de/organizations/100092306",
                  "LC":"https://data.slub-dresden.de/organizations/100822142",
                  "DDB":"https://data.slub-dresden.de/organizations/824631854",
                  "WIKIDATA":"https://www.wikidata.org/wiki/Q2013",
                  "BNF":"https://data.slub-dresden.de/organizations/188898441",
                  "dewiki":"http://de.wikipedia.org",
                  "enwiki":"http://en.wikipedia.org",
                  "DE-611":"https://data.slub-dresden.de/organizations/103675612"
                  }
    
    try:
        changed = False
        for url in ef_instances:
            r = get(url+str(gnd))
            if r.ok:
                data = r.json()
                if data.get("_source"):
                    data = data.get("_source")
                sameAsses = []  # ba-dum-ts
                if data.get("sameAs") and isinstance(data["sameAs"], list):
                    for sameAs in data.get("sameAs"):
                        if sameAs.get("@id"):
                            if not sameAs.get("@id").startswith("https://d-nb.info"):
                                obj={'@id':sameAs.get("@id"),
                                                  'publisher':{'abbr':sameAs["collection"]["abbr"],
                                                               'preferredName':sameAs["collection"]["name"]},
                                     "isBasedOn": {"@type": "Dataset",
                                                               "@id":"http://hub.culturegraph.org/entityfacts/{}".format(gnd)
                                                    }
                                    }
                                if obj["publisher"]["abbr"] in abbrevations:
                                    obj["publisher"]["@id"]=abbrevations[obj["publisher"]["abbr"]]
                                sameAsses.append(obj)
                if sameAsses:
                    record["sameAs"] = litter(record.get("sameAs"), sameAsses)
                    changed = True
                break
        return record if changed else None
    except:
        time.sleep(5)
        return entityfacts(record, gnd, ef_instances)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='enrich ES by EF!')
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
    # no, i don't steal the syntax from esbulk...
    parser.add_argument('-searchserver', type=str,
                        help="use http://host:port/index/type/id?pretty. overwrites host/port/index/id/pretty")
    parser.add_argument('-server', type=str,
                        help="use http://host:port/index/type/id?pretty. overwrites host/port/index/id/pretty")
    parser.add_argument('-stdin', action="store_true",
                        help="get data from stdin")
    parser.add_argument('-pipeline', action="store_true",
                        help="output every record (even if not enriched) to put this script into a pipeline")
    args = parser.parse_args()
    if args.server:
        slashsplit=args.server.split("/")
        args.host=slashsplit[2].rsplit(":")[0]
        search_host=args.host
        if isint(args.server.split(":")[2].rsplit("/")[0]):
            args.port=args.server.split(":")[2].split("/")[0]
            search_port=args.port
        args.index=args.server.split("/")[3]
        if len(slashsplit)>4:
            args.type=slashsplit[4]
        if len(slashsplit)>5:
            if "?pretty" in args.server:
                tabbing=4
                args.id=slashsplit[5].rsplit("?")[0]
            else:
                args.id=slashsplit[5]
    if args.searchserver:
        slashsplit=args.searchserver.split("/")
        search_host=slashsplit[2].rsplit(":")[0]
        if isint(args.searchserver.split(":")[2].rsplit("/")[0]):
            search_port=args.searchserver.split(":")[2].split("/")[0]
        search_index=args.searchserver.split("/")[3]
        if len(slashsplit)>4:
            search_type=slashsplit[4]
            
    ef_instances = ["http://"+search_host+":"+search_port +
                    "/ef/gnd/", "http://hub.culturegraph.org/entityfacts/"]
    if args.stdin:
        for line in sys.stdin:
            rec = json.loads(line)
            gnd = None
            record = None
            if isinstance(rec.get("sameAs"), list) and "d-nb.info" in str(rec.get("sameAs")):
                    for item in rec.get("sameAs"):
                        if "d-nb.info" in item["@id"] and len(item.split("/")) > 4:
                            gnd = item["@id"].rstrip().split("/")[-1]
            if gnd:
                record = entityfacts(rec, gnd, ef_instances)
                if record:
                    rec = record
            if record or args.pipeline:
                print(json.dumps(rec, indent=None))
    else:
        for rec in esgenerator(host=args.host, port=args.port, index=args.index, type=args.type, headless=True, body={"query": {"prefix": {"sameAs.@id.keyword": "https://d-nb.info"}}}, verbose=True):
            gnd = None
            record = None
            if isinstance(rec.get("sameAs"), list):
                for item in rec.get("sameAs"):
                    if "d-nb.info" in item["@id"] and len(item["@id"].split("/")) > 4:
                        gnd = item["@id"].split("/")[-1]
            if gnd:
                record = entityfacts(rec, gnd, ef_instances)
                if record:
                    rec = record
            if record or args.pipeline:
                print(json.dumps(rec, indent=None))
