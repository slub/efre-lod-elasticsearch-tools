#!/usr/bin/python3
import argparse
from es2json import esgenerator
from es2json import esfatgenerator
from es2json import esidfilegenerator
from es2json import ArrayOrSingleValue
from es2json import eprint
from es2json import litter
from es2json import isint
from requests import get
from elasticsearch import Elasticsearch, helpers
import json
import sys

es=None

mapping=None

mapping={"author":{         "fields":["hasOccupation","about","workLocation"],
                            "index":["persons","fidmove-enriched-aut"]},
         "contributor":{    "fields":["hasOccupation","about","workLocation","geo","adressRegion"],
                            "index":["persons","orga","fidmove-enriched-aut"]},
         "workLocation":{   "fields":["geo","adressRegion","sameAs"],
                            "index":["geo"]},
         "location":{   "fields":["geo","adressRegion","sameAs"],
                            "index":["geo"]},
         "mentions":{       "fields":["geo","adressRegion"],
                            "index":["geo"]},
         "relatedEvent":{   "fields":["adressRegion","location","geo"],
                            "index":["fidmove-enriched-aut"]}
         }




def isAlive(node):
    if "deathDate" or "deathPlace" in node:
        return False
    else:
        return True

def enrich_record(node,entity,host,port):
    toDel=[]
    for field in mapping:
        if isinstance(node.get(field),dict):
            ret=enrich_record(node.get(field),field,host,port)
            if ret:
                node[field]=ret
        elif isinstance(node.get(field),list):
            for n,fld in enumerate(node.get(field)):
                ret=enrich_record(fld,field,host,port)
                if ret:
                    node[field][n]=ret
                else:
                    del node[field][n]
        else:
            continue
    for item in toDel:
        node.pop(item)
    if entity:
        if node.get("@id"):
            for index in mapping[entity].get("index"):
                #print(node.get("@id"))
                url="http://"+host+":"+port+"/"+index+"/schemaorg/"+node.get("@id").split("/")[-1]
                r=get(url)
                #if entity=="relatedEvent":
                    #print(r,url)
                if ( r.ok and entity!="person" ) or ( r.ok and entity=="person" and isAlive(r.json().get("_source")) ):
                    for key in mapping[entity]["fields"]:
                        if ( key in r.json().get("_source") and not node.get(key)) or ( key in r.json().get("_source") and len(r.json().get("_source"))>len(node.get(key)) ):
                            #if node.get(key):
                            #    print(len(r.json().get("_source").get(key)),len(node.get(key)))
                            node[key]=r.json().get("_source").get(key)
                elif r.ok and not isAlive(r.json().get("_source")):
                    return None
        elif node.get("sameAs") and isinstance(node.get("sameAs"),str):
            search=es.search(index=",".join(mapping[entity]["index"]),doc_type="schemaorg",body={"query":{"term":{"sameAs.keyword":node.get("sameAs")}}},_source=True)
            if search.get("hits").get("total")>0:
                    for response in search.get("hits").get("hits"):
                        data=response.get("_source")
                        if isAlive(data) or entity!="person":
                            for key in mapping[entity]["fields"]:
                                if data.get(key):
                                    node[key]=data.get(key)
                            break
                        elif not isAlive(data) and entity=="person":
                            break
                
        elif node.get("sameAs") and isinstance(node.get("sameAs"),list):
            for sameAs in node.get("sameAs"):
                search=es.search(index=",".join(mapping[entity]["index"]),doc_type="schemaorg",body={"query":{"term":{"sameAs.keyword":sameAs}}},_source=True)
                if search.get("hits").get("total")>0:
                    for response in search.get("hits").get("hits"):
                        data=response.get("_source")
                        if isAlive(data) or entity!="person":
                            for key in mapping[entity]["fields"]:
                                if data.get(key):
                                    node[key]=data.get(key)
                            break
                        elif not isAlive(data) and entity=="person":
                            break
        else:
            pass
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
    parser.add_argument('-idfile',type=str,help="path to a file with IDs to process")
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
            newrecord=enrich_record(json.loads(line),None,args.host,str(args.port))
            if newrecord.get("author") or newrecord.get("contributor"):
                print(json.dumps(enrich_record(newrecord,None,args.host,str(args.port)),indent=tabbing))
    elif args.idfile:
        for record in esidfilegenerator(host=args.host,port=9200,index=args.index,type=args.type,body=None,source=True,source_exclude=None,source_include=None,headless=True,idfile=args.idfile):
            #recstrlen=len(json.dumps(record,indent=None))
            #print(json.dumps(record,indent=tabbing),"\n")
            length=len(json.dumps(record,indent=None))
            newrecord=enrich_record(record,None,args.host,str(args.port))
            dumpstring=str(json.dumps(newrecord,indent=None))
            if len(dumpstring) > length:
            #if newrecord.get("author") or newrecord.get("contributor"):
                print(json.dumps(newrecord,indent=tabbing))
    
    else:
        for record in esgenerator(host=args.host,port=9200,index=args.index,id=args.id,type=args.type,body={"query":{"bool":{"must":{"range":{"datePublished":{"gt":1990,"lt":2020,"boost":2}}},"should":[{"exists":{"field":"author"}},{"exists":{"field":"contributor"}}]}}}
                                                                                                    ,source=True,source_exclude=None,source_include=None,headless=True):
            #recstrlen=len(json.dumps(record,indent=None))
            #print(json.dumps(record,indent=tabbing),"\n")
            #length=len(json.dumps(record,indent=None))
            newrecord=enrich_record(record,None,args.host,str(args.port))
            #print(length,len(json.dumps(newrecord,indent=None)))
            dumpstring=str(json.dumps(newrecord,indent=None))
            #if len(dumpstring) > length:
            if newrecord and ( newrecord.get("author") or newrecord.get("contributor") ):
                print(dumpstring)
            
