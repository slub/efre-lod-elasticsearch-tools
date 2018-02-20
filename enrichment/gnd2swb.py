#!/usr/bin/python3
# -*- coding: utf-8 -*-
import elasticsearch
from elasticsearch import Elasticsearch
import json
from pprint import pprint
import argparse
import sys
import os.path
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

### replace DNB IDs by SWB IDs in your ElasticSearch Index.
### example usage: ./gnd2swb.py -host ELASTICSEARCH_SERVER -index swbfinc -type finc -aut_index=source-schemaorg -aut_type schemaorg
def handle_author(author):
    try:
        data=es.get(index=args.aut_index,doc_type=args.aut_type,id=author,_source="@id")
    except elasticsearch.exceptions.NotFoundError:
        return None
    if "@id" in data["_source"]:
        return data["_source"]["@id"]
    
def process_stuff(jline):
    changed=False
    if "author" in jline["_source"]:
        if isinstance(jline["_source"]["author"],str):
            author_id=handle_author(jline["_source"]["author"])
            if author_id:
                jline["_source"]["author"]=author_id
                changed=True
        elif isinstance(jline["_source"]["author"],dict):
            if "@id" in jline["_source"]["author"]:
                author_id=handle_author(jline["_source"]["author"]["@id"])
                if author_id:
                    jline["_source"]["author"]["@id"]=author_id
                    changed=True
        elif isinstance(jline["_source"]["author"],list):
            for author in jline["_source"]["author"]:
                if isinstance(author,dict):
                    if author_gnd_key in author:
                        author_id=handle_author(author["@id"])
                        if author_id:
                            author["@id"]=author_id
                            changed=True
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

def getidbygnd(gnd):
    indices=[{"index":"orga",
              "type" :"schemaorg"},
             {"index":"persons",
              "type" :"schemaorg"},
             {"index":"geo",
              "type" :"schemaorg"},
             {"index":"resources",
              "type" :"schemaorg"}]
    if gnd.startswith("(DE-588)"):
        uri=gnd2uri(gnd)
    elif gnd.startswith("http://d"):
        uri=gnd
    else:
        uri=checkurl(gnd)
        if not uri:
            return
    for elastic in indices:
        print(elastic)
        http = urllib3.PoolManager()
        url="http://194.95.145.44:9200/"+elastic["index"]+"/"+elastic["type"]+"/_search?q=sameAs:\""+uri+"\""
        try:
                r=http.request('GET',url)
                data = json.loads(r.data.decode('utf-8'))
                if 'Error' in data or not 'hits' in data:
                    return None
        except:
            return None
        if data['hits']['total']!=1:
            return None
        for hit in data["hits"]["hits"]:
            if "_id" in hit:
                return str("http://data.slub-dresden.de/"+str(elastic["index"])+"/"+hit["_id"])
    
        
        

if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='replace DNB IDs (GND) by SWB IDs in your ElasticSearch Index!')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-aut_index',type=str,help="Authority-Index")
    parser.add_argument('-aut_type',type=str,help="Authority-Type")
    parser.add_argument('-mp',action='store_true',help="Enable Multiprocessing")
    args=parser.parse_args()
    es=Elasticsearch(host=args.host)
    if args.mp:
        pool = Pool(32)
        pool.map(process_stuff, esgenerator(host=args.host,port=args.port,type=args.type,source="author",index=args.index,headless=False))
    else:
        for record in esgenerator(host=args.host,port=args.port,type=args.type,index=args.index,headless=False):
            process_stuff(record)
    
