#!/usr/bin/python3

import argparse
import json
import sys
import requests
from es2json import esgenerator,isint,litter,eprint,esidfilegenerator
from rdflib import Graph

def enrichrecord(record,hit,search_host,search_port):
    if "http://id.loc.gov/vocabulary/relators/isb" in hit.get("_source"):
        for subject in hit.get("_source").get("http://id.loc.gov/vocabulary/relators/isb"):
            if subject.get("@id").startswith("http://d-nb.info/gnd/"):
                placeOfBusinnes=requests.get("http://{host}:{port}/gnd-records/record/{gndId}".format(host=search_host,port=search_port,gndId=subject.get("@id").split("/")[-1]))
                if placeOfBusinnes.ok and placeOfBusinnes.json().get("_source") and placeOfBusinnes.json().get("_source").get("placeOfBusiness"):
                    pobId=placeOfBusinnes.json().get("_source").get("placeOfBusiness")[0]
                    if pobId.startswith("http://d-nb.info/gnd/"):
                        place=requests.get("http://data.slub-dresden.de/gnd/geo/{gndId}".format(host=search_host,port=search_port,gndId=pobId.split("/")[-1]))
                        if place.ok:
                            placeId=place.json()
                            #print(json.dumps(placeId))
                            if not "editor" in record:
                                record["editor"]={}
                            record["editor"]["location"]={}
                            for fields in ["name","@id","sameAs","geo","adressRegion"]:
                                record["editor"]["location"][fields]=placeId[0].get(fields)
                                record["editor"]["location"][fields]=placeId[0].get(fields)
                            print(json.dumps(record))
    return record
    
def get_gndbyISSN(record,search_host,search_port,issn):
    #expect a array of issn
    changed=False
    for single_issn in issn:
        for hit in esgenerator(host=search_host,port=search_port,index="dnb-titel",type="resources",body={"query":{"match":{"http://purl.org/ontology/bibo/issn.@value.keyword":str(single_issn)}}},source=["http://id.loc.gov/vocabulary/relators/isb"],source_exclude=None,source_include=None,headless=False,timeout=60):
            record=enrichrecord(record,hit,search_host,search_port)
            changed=True
    if changed:
        return record

if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='enrich titledata by DNB-Titledata over ISSN!')
    parser.add_argument('-host',type=str,default="127.0.0.1",help='hostname or IP-Address of the ElasticSearch-node to use, default is localhost.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-index',type=str,help='ElasticSearch Search Index to use')
    parser.add_argument('-type',type=str,help='ElasticSearch Search Index Type to use')
    parser.add_argument("-id",type=str,help="retrieve single document (optional)")
    parser.add_argument('-stdin',action="store_true",help="get data from stdin")
    parser.add_argument('-pipeline',action="store_true",help="output every record (even if not enriched) to put this script into a pipeline")
    parser.add_argument('-server',type=str,help="use http://host:port/index/type/id?pretty. overwrites host/port/index/id/pretty") #no, i don't steal the syntax from esbulk...
    parser.add_argument('-searchserver',type=str,help="search instance to use. default is -server e.g. http://127.0.0.1:9200")
    parser.add_argument('-idfile',type=str,help="path to a file with IDs to process")
    args=parser.parse_args()
    if args.server:
        slashsplit=args.server.split("/")
        args.host=slashsplit[2].rsplit(":")[0]
        if isint(args.server.split(":")[2].rsplit("/")[0]):
            args.port=args.server.split(":")[2].split("/")[0]
        args.index=args.server.split("/")[3]
        if len(slashsplit)>4:
            args.type=slashsplit[4]
        if len(slashsplit)>5:
            if "?pretty" in args.server:
                args.pretty=True
                args.id=slashsplit[5].rsplit("?")[0]
            else:
                args.id=slashsplit[5]
    if args.searchserver:
        slashsplit=args.searchserver.split("/")
        search_host=slashsplit[2].rsplit(":")[0]
        if isint(args.searchserver.split(":")[2].rsplit("/")[0]):
            search_port=args.searchserver.split(":")[2].split("/")[0]
    else:
        search_host=args.host
        search_port=args.port
    for rec in esgenerator(host=args.host,port=args.port,index=args.index,type=args.type,id=args.id,headless=True,body={"query": {"exists":{"field": "partOfSeries.@id"}}}):
        for seriesAttr in rec.get("partOfSeries"):
            if "@id" in seriesAttr:
                series=requests.get("http://{host}:{port}/resources/schemaorg/{_id}".format(host=args.host,port=args.port,_id=seriesAttr.get("@id").split("/")[-1]))
                if series.json().get("_source") and series.json().get("_source").get("issn"):
                    record=get_gndbyISSN(rec,search_host,search_port,series.json().get("_source").get("issn"))
                    if record:
                        pass
                        #print(record)
            #if args.pipeline or record:
                #print(json.dumps(rec,indent=None))
            
