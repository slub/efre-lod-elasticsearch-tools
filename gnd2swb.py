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
sys.path.append('~/slub-lod-elasticsearch-tools/')
from es2json import esgenerator
from es2json import eprint


es=None
args=None

author_gnd_key="sameAs"

### replace DNB IDs by SWB IDs in your ElasticSearch Index.
### example usage: ./gnd2swb.py -host ELASTICSEARCH_SERVER -index swbfinc -type finc -aut_index=source-schemaorg -aut_type schemaorg
def handle_author(author):
    http = urllib3.PoolManager()
    url="http://"+str(args.host)+":"+str(args.port)+"/"+str(args.aut_index)+"/"+str(args.aut_type)+"/_search?q=sameAs:\""+str(author)+"\""
    try:
        r=http.request('GET',url)
        data = json.loads(r.data.decode('utf-8'))
        if data["hits"]["total"]==1:
            for hits in data["hits"]["hits"]:
                return str(hits["_source"]["@id"])
    except:
            eprint(url)
    return None
    
def process_stuff(jline):
    tes=Elasticsearch(host=args.host)
    changed=False
    if "author" in jline:
        if isinstance(jline["author"],str):
            author_id=handle_author(jline["author"])
            if author_id:
                jline["author"]=author_id
                changed=True
        elif isinstance(jline["author"],dict):
            if "@id" in jline["author"]:
                author_id=handle_author(jline["author"]["@id"])
                if author_id:
                    jline["author"]["@id"]=author_id
                    changed=True
        elif isinstance(jline["author"],list):
            for author in jline["author"]:
                if isinstance(author,dict):
                    if author_gnd_key in author:
                        author_id=handle_author(author["@id"])
                        if author_id:
                            author["@id"]=author_id
                            changed=True
    if changed:
        tes.index(index=args.index,doc_type=args.type,body=jline,id=jline["identifier"])

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
    if args.mp:
        pool = Pool(32)
        pool.map(process_stuff, esgenerator(host=args.host,port=args.port,type=args.type,index=args.index,headless=True))
    else:
        for record in esgenerator(host=args.host,port=args.port,type=args.type,index=args.index,headless=True):
            process_stuff(record)
    
