#!/usr/bin/python3
import argparse
from es2json import esgenerator
from es2json import esfatgenerator
from es2json import ArrayOrSingleValue
from es2json import eprint
from es2json import litter
from es2json import isint
from requests import get
from elasticsearch import Elasticsearch, helpers
import json
import sys

es=None

mapping={"author":{         "fields":["relatedTo","hasOccupation","about","workLocation"],
                            "index":"persons"},
         "contributor":{    "fields":["relatedTo","hasOccupation","about","workLocation"],
                            "index":"persons"},
         "workLocation":{   "fields":["geo","adressRegion"],
                            "index":"geo"},
         "mentions":{       "fields":["geo","adressRegion"],
                            "index":"geo"}
         }

def enrich_record(node,entity,host,port):
    change=False
    if entity:
        if node.get("@id"):
            r=get("http://"+host+":"+port+"/"+mapping[entity]["index"]+"/schemaorg/"+node.get("@id").split("/")[5])
            if r.ok:
                for key in mapping[entity]["fields"]:
                    if key in r.json().get("_source") and key not in node:
                        node[key]=r.json().get("_source").get(key)
        elif node.get("sameAs") and isinstance(node.get("sameAs"),str):
            search=es.search(index=mapping[entity]["index"],doc_type="schemaorg",body={"query":{"term":{"sameAs.keyword":node.get("sameAs")}}},_source=True)
            if search.get("hits").get("total")>0:
                response=search.get("hits").get("hits")[0].get("_source")
                for key in mapping[entity]["fields"]:
                    if response.get(key):
                        node[key]=response[key]
        elif node.get("sameAs") and isinstance(node.get("sameAs"),list):
            for sameAs in node.get("sameAs"):
                search=es.search(index=mapping[entity]["index"],doc_type="schemaorg",body={"query":{"term":{"sameAs.keyword":sameAs}}},_source=True)
                if search.get("hits").get("total")>0:
                    response=search.get("hits").get("hits")[0].get("_source")
                    for key in mapping[entity]["fields"]:
                        if response.get(key):
                            node[key]=response[key]
                    break
        else:
            pass
    for field in mapping:
        if isinstance(node.get(field),dict):
            node[field]=enrich_record(node.get(field),field,host,port)
        elif isinstance(node.get(field),list):
            for n,fld in enumerate(node.get(field)):
                node[field][n]=enrich_record(fld,field,host,port)
        else:
            continue
    return node

if __name__ == "__main__":
    #argstuff
    parser=argparse.ArgumentParser(description='Enrich Resources with authority Data for FID-Move')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use. If None we try to read ldj from stdin.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-id',type=str,help='map single document, given by id')
    parser.add_argument('-help',action="store_true",help="print this help")
    parser.add_argument('-stdin',action="store_true",help="If you want to read from stdin")
    parser.add_argument('-debug',action="store_true",help='Dump processed Records to stdout (mostly used for debug-purposes)')
    parser.add_argument('-server',type=str,help="use http://host:port/index/type/id?pretty syntax. overwrites host/port/index/id/pretty")
    parser.add_argument('-pretty',action="store_true",default=False,help="output tabbed json")
    args=parser.parse_args()
    
    if args.server:
        slashsplit=args.server.split("/")
        args.host=slashsplit[2].rsplit(":")[0]
        if isint(args.server.split(":")[2].rsplit("/")[0]):
            args.port=args.server.split(":")[2].split("/")[0]
        if len(slashsplit)>3:
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
    if args.pretty:
        tabbing=4
    else:
        tabbing=None
    es=Elasticsearch([{'host':args.host}],port=args.port)
    
    if args.stdin:
        for line in sys.stdin:
            length=len(line.rstrip())
            newrecord=enrich_record(json.loads(line),None,args.host,str(args.port))
            dumpstring=json.dumps(newrecord,indent=None)
            if len(dumpstring) > length:
                print(dumpstring)
    else:
        for record in esgenerator(host=args.host,port=9200,index=args.index,id=args.id,type=args.type,body=None,source=True,source_exclude=None,source_include=None,headless=True):
            #length=len(json.dumps(record,indent=None))
            #newrecord=enrich_record(record,None,args.host,str(args.port))
            #dumpstring=str(json.dumps(newrecord,indent=None))
            #if len(dumpstring) > length:
            print(json.dumps(enrich_record(record,None,args.host,str(args.port)),indent=tabbing))
                                        
                
