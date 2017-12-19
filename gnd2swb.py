#!/usr/bin/python3
# -*- coding: utf-8 -*-
from datetime import datetime
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
sys.path.append('~/slub-lod-elasticsearch-tools/')
from getindex import esgenerator
from getindex import eprint


es=None
args=None

### replace DNB IDs by SWB IDs in your ElasticSearch Index.
### example usage: ./gnd2swb.py -host ELASTICSEARCH_SERVER -index swbfinc -type finc -aut_index=source-schemaorg -aut_type schemaorg

def process_stuff(jline):
    tes=Elasticsearch(host=args.host)
    if "author" in jline:
        for author in jline["author"]:
            if "@id" in author:
                http = urllib3.PoolManager()
                try:
                    url="http://"+str(args.host)+":"+str(args.port)+"/"+str(args.aut_index)+"/"+str(args.aut_type)+"/_search?q=sameAs:\""+str(author["@id"])+"\""
                    r=http.request('GET',url)
                    data = json.loads(r.data.decode('utf-8'))
                except:
                    print(url)
                if data["hits"]["total"]==1:
                    for hits in data["hits"]["hits"]:
                        author["@id"]=hits["_source"]["@id"]
                        tes.index(index=args.index,doc_type=args.type,body=jline,id=jline["identifier"])
    

if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='enrich your ElasticSearch Search Index with data from entityfacts!')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-aut_index',type=str,help="Authority-Index")
    parser.add_argument('-aut_type',type=str,help="Authority-Type")
    parser.add_argument('-mp',action='store_true',help="Enable Multiprocessing")
    args=parser.parse_args()
    
    if args.mp:
        pool = Pool(128)
        pool.map(process_stuff, esgenerator(host=args.host,port=args.port,type=args.type,index=args.index,headless=True))
    else:
        for record in esgenerator(host=args.host,port=args.port,type=args.type,index=args.index,headless=True):
            process_stuff(record)
    
