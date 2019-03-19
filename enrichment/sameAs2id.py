#!/usr/bin/python3
# -*- coding: utf-8 -*-
import elasticsearch
from elasticsearch import Elasticsearch, helpers
import json
from pprint import pprint
import argparse
import sys
import os.path
import requests
import signal
import urllib3.request
from multiprocessing import Lock, Pool, Manager
from es2json import esgenerator
from es2json import eprint
from esmarc import gnd2uri
from esmarc import isint
from esmarc import ArrayOrSingleValue
from es2json import simplebar

es=None

map_id={"additionalType":"",
        "about":["tags"],
        "areaServed":["geo"],
        "fromLocation":["geo"],
        "location":["geo"],
        "parentOrganization":["orga"],
        "contentLocation":["geo"],
        "participant":["persons"],
        "relatedTo":["persons","orga","tags"],
        "about":["tags","persons","orga"],
        "author":["persons","orga"],
        "contributor":["persons","orga"],
        "mentions":["tags","orga","persons","geo"],
        "colleague":["persons"],
        "knows":["persons"],
        "locationCreated":["geo"],
        "recipent":["persons"],
        "spouse":["persons"],
        "birthPlace":["geo"],
        "children":["persons"],
        "deathPlace":["geo"],
        "follows":["persons"],
        "honorificPrefix":["tags"],
        "parent":["persons"],
        "sibling":["persons"],
        "workLocation":["geo","orga"],
        "description":["tags"],
        "partOfSeries":["resources"]
} 
def handlesameAs(record,field,host,port):
    if record and field and host and port and map_id.get(field):
        changed=True
        hits=[]
        index=','.join(map_id.get(field))
        if isinstance(record.get("sameAs"),list):
            for elem in record["sameAs"]:
                search=es.search(index=index,doc_type="schemaorg",body={"query":{"term":{"sameAs.keyword":elem}}},_source=False)
                if search.get("hits").get("total")>0:
                    hits+=search.get("hits").get("hits")
                #print(record.get("sameAs"),field)
                #print(json.dumps(search.get("hits").get("hits"),indent=4))
        elif isinstance(record.get("sameAs"),str):
            search=es.search(index=index,doc_type="schemaorg",body={"query":{"term":{"sameAs.keyword":record.get("sameAs")}}},_source=False)
            if search.get("hits").get("total")>0:
                hits+=search.get("hits").get("hits")
        newlist = sorted(hits, key=lambda k: k['_score'],reverse=True) 
        if newlist:
            return str("http://data.slub-dresden.de/"+newlist[0].get("_index")+"/"+newlist[0].get("_type")+"/"+newlist[0].get("_id"))
    return None

           
def enrich_sameAs(hit,search_host,search_port,pipeline):
    change=False
    for field in hit:
        if isinstance(hit.get(field),list):
            for n,elem in enumerate(hit.get(field)):
                if "sameAs" in elem:
                    _id=handlesameAs(elem,field,search_host,search_port)
                    if _id:
                        hit[field][n]["@id"]=_id
                        change=True
        elif isinstance(hit.get(field),dict) and "sameAs" in hit.get(field):
            _id=handlesameAs(hit.get(field),field,search_host,search_port)
            if _id:
                hit[field]["@id"]=_id
                change=True
        else:
           continue
    if change or pipeline:
        print(json.dumps(hit,indent=None))

    
if __name__ == "__main__":
    #argstuff
    parser=argparse.ArgumentParser(description='Resolve sameAs of GND/SWB to your own IDs.')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use. If None we try to read ldj from stdin.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-help',action="store_true",help="print this help")
    parser.add_argument('-stdin',action="store_true",help="get data from stdin")
    parser.add_argument('-id',type=str,help="enrich a single id")
    parser.add_argument('-server',type=str,help="use http://host:port/index/type/id syntax. overwrites host/port/index/id/pretty")
    parser.add_argument('-searchserver',type=str,help="search instance to use. default is -server e.g. http://127.0.0.1:9200")
    parser.add_argument('-pipeline',action="store_true",help="output every record (even if not enriched) to put this script into a pipeline")
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
    if args.help:
        parser.print_help(sys.stderr)
        exit()
    
    if args.searchserver:
        slashsplit=args.searchserver.split("/")
        search_host=slashsplit[2].rsplit(":")[0]
        if isint(args.searchserver.split(":")[2].rsplit("/")[0]):
            search_port=args.searchserver.split(":")[2].split("/")[0]
    else:
        search_host=args.host
        search_port=args.port
    es=Elasticsearch([{'host':search_host}],port=search_port)
    
    if args.stdin:
        for line in sys.stdin:
            hit=json.loads(line)
            enrich_sameAs(hit,search_host,search_port,args.pipeline)
            
    else:
        for hit in esgenerator(host=args.host,port=args.port,index=args.index,type=args.type,headless=True):
            enrich_sameAs(hit,search_host,search_port,args.pipeline)
 
