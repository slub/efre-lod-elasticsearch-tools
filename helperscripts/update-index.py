#!/usr/bin/python3
# -*- coding: utf-8 -*-
import elasticsearch
from elasticsearch import Elasticsearch
import json
from pprint import pprint
import argparse
import sys
import itertools

es=None
args=None


def merge_lists(a,b):
    c=[]
    if isinstance(a,list):
        for item in itertools.chain(a,b):
            if item not in c:
                c.append(item)
                return b
    else:
        if a not in b:
            b.append(a)
            return b
        else:
            return a

def merge_dicts(a,b):
    change=False
    for k,v in a:
        if k in b:
            if isinstance(b[k],list):
                value=merge_lists(a[k],b[k])
                if value:
                    b[k]=value
                change=True
            elif isinstance(b[k],dict) and isinstance(a[k],dict):
                value=merge_dicts(a[k],b[k])
                if value:
                    b[k]=value
                change=True
            elif isinstance(b[k],str) and isinstance(a[k],str):
                if len(b[k])<len(a[k]):
                    b[k]=a[k]
                    change=True
        else:
                b[k]=a[k]
                change=True
    if change:
        return b
    else:
        return None
    
                    
    
def process_stuff(jline):
    tes=Elasticsearch(host=args.host)
    es_doc=tes.get(index=args.index,doc_type=args.type,id=args.id)
    if "_source" in es_doc:
        body=merge_dicts(jline,es_doc["_source"])
        if body:
            tes.update(index=args.index,doc_type=args.type,id=args.id,body=body)
        else:
            return
    
    
if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='update an index (instead of re-indexing)')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-id',type=str,help='_id field to use to identify the unique documents')
    args=parser.parse_args()
    input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    for line in input_stream:
        process_stuff(json.loads(line))
        
    
