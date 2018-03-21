#!/usr/bin/python3
# -*- coding: utf-8 -*-
import elasticsearch
from multiprocessing import Pool
from elasticsearch import Elasticsearch, exceptions, helpers
from es2json import eprint
import io
import os
import json
import signal
from pprint import pprint
import argparse
import sys
import itertools
from gnd2swb import simplebar

sb=None
args=None
es=None
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
    
                    
    
def update_index(jline,args):
    tes=Elasticsearch(host=args.host)
    es_doc=tes.get(index=args.index,doc_type=args.type,id=args.id)
    if "_source" in es_doc:
        body=merge_dicts(jline,es_doc["_source"])
        if body:
            tes.update(index=args.index,doc_type=args.type,id=args.id,body=body)
        else:
            return
        
actions=[]

def update_marc_index(jline):
    PPN=None
    ts=None
    if "001" in jline:
        PPN=jline["001"][0]
    if "005" in jline:
        ts=jline["005"][0]
    if PPN and ts:
        try:
            esdata=es.get(index=args.index,id=PPN,doc_type=args.type,_source="005")
            if "_source" in esdata and "005" in esdata["_source"]:
                if float(esdata["_source"]["005"][0])<float(ts):
                    actions.append({'_op_type':"update",
                                    '_index':args.index,
                                    '_type':args.type,
                                    '_id':PPN,
                                    'doc':jline})
                    sb.update()
                else:
                    sb.update()
        except exceptions.NotFoundError:
            actions.append({'_op_type':"index",
                                    '_index':args.index,
                                    '_type':args.type,
                                    '_id':PPN,
                                    'doc':jline})
            sb.update()
    if len(actions)==1000:
        helpers.bulk(es,actions,stats_only=True)
        actions.clear()

if __name__ == "__main__":
    parser=argparse.ArgumentParser(description='update an index (instead of re-indexing)')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-id',type=str,help='_id field to use to identify the unique documents')
    parser.add_argument('-i',type=str,required=True,help='path to input file')
    args=parser.parse_args()    
    es=Elasticsearch([{'host':args.host}],port=args.port)
    with open(args.i,"r") as fd:
        sb=simplebar()
        #for line in fd:
        #    try:
        #        update_marc_index(json.loads(line),args,es)
        #    except json.decoder.JSONDecodeError:
        #        eprint(line)
        pool = Pool()
        pool.map(update_marc_index,fd)
        
        
    
