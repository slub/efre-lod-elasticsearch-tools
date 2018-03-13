#!/usr/bin/python3
# -*- coding: utf-8 -*-
import elasticsearch
from elasticsearch import Elasticsearch
import json
from pprint import pprint
import argparse
import sys
import os.path
import requests
import signal
import urllib3.request
from multiprocessing import Pool
from es2json import esgenerator
from es2json import eprint
from esmarc import gnd2uri
from esmarc import ArrayOrSingleValue

es=None
args=None

author_gnd_key="sameAs"

### replace SWB/GND IDs by SWB IDs in your ElasticSearch Index.
### example usage: ./gnd2swb.py -host ELASTICSEARCH_SERVER -index swbfinc -type finc -aut_index=source-schemaorg -aut_type schemaorg
def handle_author(author):
    try:
        data=es.get(index=args.aut_index,doc_type=args.aut_type,id=author,_source="@id")
    except elasticsearch.exceptions.NotFoundError:
        return None
    if "@id" in data["_source"]:
        return data["_source"]["@id"]
    
    if changed:
        es.index(index=args.index,doc_type=args.type,body=jline["_source"],id=jline["_id"])

def checkurl(gnd):
    url="https://d-nb.info/gnd/"+str(gnd)
    c = urllib3.PoolManager()
    c.request("HEAD", '')
    if c.getresponse().status == 200:
        return url
    else:
        return None

def getidbygnd(gnd,cache=None):
    if cache:
        if gnd in cache:
            return cache[gnd]
    indices=[{"index":"orga",
              "type" :"schemaorg"},
             {"index":"persons",
              "type" :"schemaorg"},
             {"index":"geo",
              "type" :"schemaorg"},
             {"index":"resources",
              "type" :"schemaorg"},
             {"index":"dnb",
              "type" :"gnd",
              "index":"ef",
              "type" :"gnd"}]
    if gnd.startswith("(DE-588)"):
        uri=gnd2uri(gnd)
    elif gnd.startswith("http://d"):
        uri=gnd
    else:
        uri=checkurl(gnd)
        if not uri:
            return
    for elastic in indices:
        http = urllib3.PoolManager()
        url="http://"+args.host+":9200/"+elastic["index"]+"/"+elastic["type"]+"/_search?q=sameAs:\""+uri+"\""
        try:
                r=http.request('GET',url)
                data = json.loads(r.data.decode('utf-8'))
                if 'Error' in data or not 'hits' in data:
                    continue
        except:
            continue
        if data['hits']['total']!=1:
            continue
        for hit in data["hits"]["hits"]:
            if "_id" in hit:
                if cache:
                    cache[gnd]=str("http://data.slub-dresden.de/"+str(elastic["index"])+"/"+hit["_id"])
                    return cache[gnd]
                else:
                    return str("http://data.slub-dresden.de/"+str(elastic["index"])+"/"+hit["_id"])

def useadlookup(feld,index,uri):
        r=requests.get("http://"+args.host+":8000/welcome/default/data?ind="+str(index)+"&feld="+str(feld)+"&uri="+str(uri))
        if r.ok:
            return r.json()
        else:
            return {}

def resolve_geos(record):
    new=None
    for geo in ["deathPlace","birthPlace"]:
            if geo in record:
                if isinstance(record[geo],list):
                    for elem in record[geo]:
                        _id=resolve_bnode(elem,"geo")
                        if _id and "http://data.slub-dresden.de/" in _id:
                            record[geo]=_id
                            return record
                if isinstance(record[geo],dict):
                    if _id and "http://data.slub-dresden.de/" in _id:
                        record[geo]=_id
                        return record
    return None

def resolve_persons(record):
    changed=False
    for person in ["author","relatedTo","colleague","contributor","knows","follows","parent","sibling","spouse","children"]:
        if person in record:
            eprint("resolve_persons()"+str(record[person]))
            if isinstance(record[person],list):
                for n,elem in enumerate(record[person]):
                    resolved=resolve_bnode(elem,"persons")
                    if resolved.startswith("http://data.slub-dresden.de/"):
                        record[person][n]["@id"]=None
                        record[person][n]["@id"]=litter(record[person][n]["@id"],resolved)
                        changed=True
            elif isinstance(record[person],dict):
                resolved=resolve_bnode(record[person],"persons")
                if resolved.startswith("http://data.slub-dresden.de/"):
                    record[person]["@id"]=None
                    record[person]["@id"]=litter(record[person]["@id"],resolved)
                    changed=True
            if isinstance(record[person],list): ## remove doubles by making a set out of the list and vice-versa
                record[person]=list(set(record[person]))
    if changed:
        return(record)
    else:
        return None
        
def resolve_bnode(record,index):
    if isinstance(record,dict):
        if "@id" not in record and "sameAs" in record:
            geo_record={}
            if isinstance(record["sameAs"],str):
                geo_record=useadlookup("sameAs",index,record["sameAs"])
            elif isinstance(record["sameAs"],list):
                for n,elem in enumerate(record["sameAs"]):
                    if elem.startswith("http://swb.bsz"):
                        geo_record=litter(geo_record,useadlookup("sameAs",index,elem))
                        if "@id" in geo_record:
                            break
                    elif elem.startswith("http://d-nb"):
                        geo_record=litter(geo_record,useadlookup("sameAs",index,elem))
                        if "@id" in geo_record:
                            break
            if "@id" in geo_record:
                return geo_record.get("@id")
    return ""
    
        
### avoid dublettes and nested lists when adding elements into lists
def litter(lst, elm):
    if not lst:
        lst=elm
    else:
        if isinstance(lst,str):
            lst=[lst]
        if isinstance(elm,str):
            if elm not in lst:
                lst.append(elm)
        elif isinstance(elm,list):
            for element in elm:
                if element not in lst:
                    lst.append(element)
    return lst

        
if __name__ == "__main__":
    #argstuff
    parser=argparse.ArgumentParser(description='Entitysplitting/Recognition of MARC-Records')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use. If None we try to read ldj from stdin.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-help',action="store_true",help="print this help")
    args=parser.parse_args()
    if args.help:
        parser.print_help(sys.stderr)
        exit()
    for hits in esgenerator(host=args.host,port=args.port,index=args.index,type=args.type,headless=True):
            record=resolve_persons(hits)
            if record:
                final=resolve_geos(record)
            else:
                final=resolve_geos(hits)
            if final:
                sys.stdout.write(json.dumps(final,indent=None)+"\n")
                sys.stdout.flush()

#if __name__ == "__main__":
    #parser=argparse.ArgumentParser(description='replace DNB IDs (GND) by SWB IDs in your ElasticSearch Index!')
    #parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use.')
    #parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    #parser.add_argument('-type',type=str,help='ElasticSearch Index to use')
    #parser.add_argument('-index',type=str,help='ElasticSearch Type to use')
    #parser.add_argument('-aut_index',type=str,help="Authority-Index")
    #parser.add_argument('-aut_type',type=str,help="Authority-Type")
    #parser.add_argument('-mp',action='store_true',help="Enable Multiprocessing")
    #args=parser.parse_args()
    #es=Elasticsearch(host=args.host)
    #if args.mp:
        #pool = Pool(32)
        #pool.map(process_stuff, esgenerator(host=args.host,port=args.port,type=args.type,source="author",index=args.index,headless=False))
    #else:
        #for record in esgenerator(host=args.host,port=args.port,type=args.type,index=args.index,headless=False):
            #process_stuff(record)
     
