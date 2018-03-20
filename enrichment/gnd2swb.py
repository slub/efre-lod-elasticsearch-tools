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
            return None

        
### avoid dublettes and nested lists when adding elements into lists
def litter(lst, elm):
    if not lst:
        lst=elm
    else:
        if isinstance(lst,str):
            lst=[lst]
        if isinstance(elm,(str,dict)):
            if elm not in lst:
                lst.append(elm)
        elif isinstance(elm,list):
            for element in elm:
                if element not in lst:
                    lst.append(element)
    return lst

map_id={ 
    "persons": ["author","relatedTo","colleague","contributor","knows","follows","parent","sibling","spouse","children"],
    "geo":     ["deathPlace","birthPlace"],
    "orga":["copyrightHolder"],
         }

def traverse(obj,path):
    if isinstance(obj,dict):
        for k,v in obj.items():
            for c,w in traverse(v,path+"."+str(k)):
                yield c,w
    elif isinstance(obj,list):
        for elem in obj:
            for c,w in traverse(elem,path+"."):
                yield c,w
    else:
        yield path,obj
        
def sameAs2ID(index,entity,record):
    changed=False
    if isinstance(record["sameAs"],str):
        r=useadlookup("sameAs",index,record["sameAs"])
        if isinstance(r,dict) and r.get("@id"):
            changed=True
            record.pop("sameAs")
            record["@id"]=r.get("@id")
    elif isinstance(record["sameAs"],list):
        record["@id"]=None
        for n,sameAs in enumerate(record['sameAs']):
            r=useadlookup("sameAs",index,sameAs)
            if isinstance(r,dict) and r.get("@id"):
                changed=True
                del record["sameAs"][n]
                record["@id"]=litter(record["@id"],r.get("@id"))
                for m,sameBs in enumerate(record["sameAs"]):
                    if sameBs in r.get("sameAs"):
                        del record["sameAs"][m]
        for checkfield in ["@id","sameAs"]:
            if not record[checkfield]:
                record.pop(checkfield)
    if changed:
        return record
    else:
        return None
                
def resolve(record,index):
    changed=False
    if index in map_id:
        for entity in map_id[index]:
            if entity in record:
                if isinstance(record[entity],list):
                    for n,sameAs in enumerate(record[entity]):
                        rec=sameAs2ID(index,entity,sameAs)
                        if rec:
                            changed=True
                            record[entity][n]=rec
                elif isinstance(record[entity],dict):
                    rec=sameAs2ID(index,entity,record[entity])
                    if rec:
                        changed=True
                        record[entity]=rec
    if changed:
        return record
    else:
        return None
        
class simplebar():
    count=0
    def __init__(self):
        self.count=0
        
    def update(self,num=None):
        if num:
            self.count+=num
        else:
            self.count+=1
        sys.stderr.write(str(self.count)+"\n"+"\033[F")
        sys.stderr.flush()
###1337 h4x:
#
#        #!/bin/bash
#for i in geo orga persons resources; do
#~/git/slub-lod-elasticsearch-tools/enrichment/gnd2swb.py -host ### args.host -index ${i} -type schemaorg | esbulk -host 194.95.145.44 -index ${i} -type schemaorg -id identifier -w 8
#done

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
        
    sb=simplebar()
    for hits in esgenerator(host=args.host,port=args.port,index=args.index,type=args.type,headless=True):
        record=hits
        changed=False
        for key in map_id:
            newrecord = resolve(record,key)
            if newrecord:
                record=newrecord
                changed=True
        if changed:
            sb.update()
            sys.stdout.write(json.dumps(record,indent=None)+"\n")
            sys.stdout.flush()
            #record=resolve(hits,"persons")
            #if record:
            #    ret=resolve(record,"geo")
            #    if ret:
            #        record=ret
            #else:
            #    ret=resolve(hits,"geo")
            #    if ret:
            #        record=ret
            #if record:
            #    sb.update()
            #    sys.stdout.write(json.dumps(record,indent=None)+"\n")
            #    sys.stdout.flush()
                
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
     
