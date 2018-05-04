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
from esmarc import ArrayOrSingleValue
from es2json import simplebar
args=None
host=None

author_gnd_key="sameAs"
map_id={ 
    "persons": ["author","relatedTo","colleague","contributor","knows","follows","parent","sibling","spouse","children"],
    "geo":     ["deathPlace","birthPlace","workLocation","location","areaServed"],
    "orga":["copyrightHolder"],
         }
### replace SWB/GND IDs by your own IDs in your ElasticSearch Index.
### example usage: ./sameAs2swb.py -host ELASTICSEARCH_SERVER -index swbfinc -type finc -aut_index=source-schemaorg -aut_type schemaorg
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
        url="http://"+host+":9200/"+str(index)+"/schemaorg/_search?_source=@id,sameAs&q="+str(feld)+":\""+str(uri)+"\""
        r=requests.get(url,headers={'Connection':'close'})
        if r.ok:
            response=r.json().get("hits")
            if response.get("total")==1:
                return response.get("hits")[0].get("_source")
            else:
                return None
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
    if "sameAs" in record and isinstance(record["sameAs"],str):
        r=useadlookup("sameAs",index,record["sameAs"])
        if isinstance(r,dict) and r.get("@id"):
            changed=True
            record.pop("sameAs")
            record["@id"]=r.get("@id")
    elif "sameAs" in record and isinstance(record["sameAs"],list):
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
        
###1337 h4x:
#
#        #!/bin/bash
#for i in geo orga persons resources; do
#~/git/slub-lod-elasticsearch-tools/enrichment/gnd2swb.py -host ### args.host -index ${i} -type schemaorg | esbulk -host 194.95.145.44 -index ${i} -type schemaorg -id identifier -w 8
#done


def work(record):
    data=resolve_uris(record.pop("_source"))
    if data:
        record.pop("_score")
        record["_op_type"]="index"
        record["_source"]=data
        if not args.debug:
            lock.acquire()
        actions.append(record)
        sys.stdout.write(".")
        if len(actions)>=1000:
            helpers.bulk(elastic,actions,stats_only=True)
            actions[:]=[]
        if not args.debug:
            lock.release()
            
def init(l,a,es):
    global lock
    global actions
    global elastic
    elastic=es
    actions=a
    lock = l
        
def resolve_uris(record):
    changed=False
    for key in map_id:
        newrecord = resolve(record,key)
        if newrecord:
            record=newrecord
            changed=True
    if changed:
        return record
    else:
        return None

if __name__ == "__main__":
    #argstuff
    parser=argparse.ArgumentParser(description='Resolve sameAs of GND/SWB to your own IDs.')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use. If None we try to read ldj from stdin.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-help',action="store_true",help="print this help")
    parser.add_argument('-id',type=str,help="enrich a single id")
    parser.add_argument('-debug',action="store_true",help="disable mp for debugging purposes")
    args=parser.parse_args()
   
    if args.help:
        parser.print_help(sys.stderr)
        exit()
    
    es=Elasticsearch([{'host':args.host}],port=args.port)
    
    host=args.host
    if args.id:
        record=es.get(index=args.index,doc_type=args.type,id=args.id).pop("_source")
        print(json.dumps(resolve_uris(record),indent=4))
    elif args.debug:
        l=[]
        a=[]
        init(l,a,es)
        for hit in esgenerator(host=args.host,port=args.port,index=args.index,type=args.type,headless=False):
            work(hit)
        if len(a)>0:
            helpers.bulk(es,a,stats_only=True)
            a[:]=[]
    else:
        with Manager() as manager:
            a=manager.list()
            l=manager.Lock()
            with Pool(16,initializer=init,initargs=(l,a,es)) as pool:
                for hit in esgenerator(host=args.host,port=args.port,index=args.index,type=args.type,headless=False):
                    pool.apply_async(work,args=(hit,))
                #pool.map(work,esgenerator(host=args.host,port=args.port,index=args.index,type=args.type,headless=False))
            
            if len(a)>0:
                helpers.bulk(es,a,stats_only=True)
                a[:]=[]
        #record=resolve_uris(hits,args)
        #if record:
            #sb.update()
            #sys.stdout.write(json.dumps(record,indent=None)+"\n")   #pipe that to esbulk
            #sys.stdout.flush()
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
     
