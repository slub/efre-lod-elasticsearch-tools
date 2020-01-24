#!/usr/bin/python3
import sys
import json
import requests
import argparse
from es2json import litter, isint, esgenerator

map = ["https://d-nb.info/standards/elementset/gnd#gndSubjectCategory",
       "https://d-nb.info/standards/elementset/gnd#fieldOfStudy",
       "https://d-nb.info/standards/elementset/gnd#fieldOfActivity",
       "https://d-nb.info/standards/elementset/gnd#biographicalOrHistoricalInformation"]


def process(record, dnb_uri, server):
    change = False  # [0]   [1] [2]         [3]   [4,-1]
    # http: / / d-nb.info / gnd / 102859268X get the GND number
    r = requests.get(server+"/gnd-records/record/"+str(dnb_uri.split("/")[-1]))
    if r.ok:
        for gndItem in map:
            if r.json().get("_source").get(gndItem):
                for elem in r.json().get("_source").get(gndItem):
                    value = elem
                    if isinstance(elem, str):
                        continue
                    elif isinstance(elem, dict):
                        if "id" in elem:
                            newvalue = elem.get("id").split("/")[-1]
                            value = elem.get("id")
                        else:
                            continue
                    newabout = {"identifier": {"propertyID": gndItem,
                                               "@type": "PropertyValue", "value": newvalue}}
                    if value.startswith("http"):
                        newabout["@id"] = value
                    if gndItem == "https://d-nb.info/standards/elementset/gnd#fieldOfStudy":
                        fos = requests.get(
                            server+"/gnd-records/record/"+newvalue)
                        if fos.ok and fos.json().get("_source").get("https://d-nb.info/standards/elementset/gnd#relatedDdcWithDegreeOfDeterminacy3"):
                            newabout["identifier"] = [
                                newabout.pop("identifier")]
                            ddcs = fos.json().get("_source").get(
                                "https://d-nb.info/standards/elementset/gnd#relatedDdcWithDegreeOfDeterminacy3")
                            if isinstance(ddcs, dict):
                                newabout["identifier"].append(
                                    {"@type": "PropertyValue", "propertyID": "DDC", "value": ddcs.get("id").split("/")[-2][:3]})
                                newabout["@id"] = ddcs.get("id")
                            elif isinstance(ddcs, list):
                                for ddc in ddcs:
                                    newabout["identifier"].append(
                                        {"@type": "PropertyValue", "propertyID": "DDC", "value": ddc.get("id").split("/")[-2][:3]})
                                    newabout["@id"] = ddc.get("id")
                            if fos.json().get("_source").get("https://d-nb.info/standards/elementset/gnd#preferredNameForTheSubjectHeading"):
                                newabout["name"] = fos.json().get("_source").get(
                                    "https://d-nb.info/standards/elementset/gnd#preferredNameForTheSubjectHeading")
                    elif gndItem == "https://d-nb.info/standards/elementset/gnd#gndSubjectCategory":
                        url = server+"/gnd-subjects/subject/_search"
                        gsc = requests.post(
                            url, json={"query": {"match": {"@id.keyword": value}}})
                        if gsc.ok and gsc.json().get("hits").get("total") == 1:
                            for hit in gsc.json().get("hits").get("hits"):
                                newabout["name"] = " ".join(hit.get("_source").get(
                                    "skos:prefLabel").get("@value").replace("\n", "").split())
                    if not record.get("about"):
                        record["about"] = newabout
                        change = True
                    else:
                        plzAdd = True
                        # print(elem,record.get("about"))
                        if isinstance(record.get("about"), dict) and record.get("about").get("@id") and value not in record.get("about").get("@id"):
                            record["about"] = [record.pop("about")]
                        elif isinstance(record.get("about"), list):
                            for item in record.get("about"):
                                if item.get("@id") and value in item.get("@id"):
                                    plzAdd = False
                                elif isinstance(item.get("identifier"), list):
                                    for ident_list_elem in item.get("identifier"):
                                        if ident_list_elem.get("@id") and value in ident_list_elem.get("@id"):
                                            plzAdd = False
                        if plzAdd:
                            change = True
                            record["about"] = litter(record["about"], newabout)
    return record if change else None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='enrich ES by GND Sachgruppen!!')
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
    # no, i don't steal the syntax from esbulk...
    parser.add_argument('-searchserver', type=str,
                        help="use http://host:port for your GND ElasticSearch Server")
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
    record = None
    if args.stdin:
        for line in sys.stdin:
            rec = json.loads(line)
            gnd = None
            if isinstance(rec.get("sameAs"), list) and "d-nb.info" in str(rec.get("sameAs")):
                for item in rec.get("sameAs"):
                    if "d-nb.info" in item["@id"] and len(item.split("/")) > 4:
                        gnd = item["@id"].rstrip().split("/")[-1]
            if gnd:
                record = process(rec, gnd, args.searchserver)
                if record:
                    rec = record
            if (record or args.pipeline) and rec:
                print(json.dumps(rec, indent=None))
    else:
        for rec in esgenerator(host=args.host, port=args.port, index=args.index, type=args.type, headless=True, body={"query": {"prefix": {"sameAs.keyword": "https://d-nb.info"}}}):
            gnd = None
            if isinstance(rec.get("sameAs"), list):
                for item in rec.get("sameAs"):
                    if "d-nb.info" in item["@id"] and len(item["@id"].split("/")) > 4:
                        gnd = item["@id"].split("/")[-1]
            if gnd:
                record = process(rec, gnd, args.searchserver)
                if record:
                    rec = record
            if record or args.pipeline:
                print(json.dumps(rec, indent=None))
